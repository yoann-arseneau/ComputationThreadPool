/* MIT License
 * 
 * Copyright (c) 2022 Yoann Arseneau
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace ComputationThreadPoolLib {
	/// <summary>
	///     Thread pool intended for computation-heavy workloads. Each worker thread is assigned a
	///     processor affinity mask to maximize throughput.
	/// </summary>
	public class ComputationThreadPool {
		const int Running = 0;
		const int Closed = 1;

		private static int ThreadPoolCount = 0;

		private Scheduler? _TaskScheduler;
		/// <summary>A scheduler for running tasks on this thread pool.</summary>
		public TaskScheduler TaskScheduler => _TaskScheduler ??= new(this);

		public bool IsClosed => _status != Running;

		private readonly ConcurrentQueue<Job> _jobQueue = new();
		private readonly ComputationWorker[] _threads;
		private readonly int _threadPoolId;
		private readonly ThreadPriority _threadPriority;
		/// <summary>Keeps track of the number of active worker threads.</summary>
		private int _runningWorkerCount;
		/// <summary>Determines whether thread pool is closed.</summary>
		/// <remarks>Synchronized using <see cref="_jobQueue"/>.</remarks>
		private int _status = Running;
		private readonly ILogger? _logger;

		/// <inheritdoc cref="ComputationThreadPool(IEnumerable{nint}, ThreadPriority, ILogger?)"/>
		public ComputationThreadPool(IEnumerable<nint> affinityMasks)
			: this(affinityMasks, default) {
		}
		/// <inheritdoc cref="ComputationThreadPool(IEnumerable{nint}, ThreadPriority, ILogger?)"/>
		public ComputationThreadPool(
				IEnumerable<nint> affinityMasks,
				ThreadPriority priority)
			: this(affinityMasks, priority, null) {
		}
		/// <summary>
		///     Initializes a new instance of the <see cref="ComputationThreadPool"/> class.
		/// </summary>
		/// <param name="affinityMasks">The affinity mask for each worker thread.</param>
		/// <param name="priority">The priority of all worker threads .</param>
		/// <param name="logger">If present, used to log otherwise-ignored exceptions.</param>
		/// <exception cref="ArgumentException">
		///     if threadMasks does not contain at least one affinity mask.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		///     if <paramref name="affinityMasks"/> is <see langword="null"/>.
		/// </exception>
		public ComputationThreadPool(
				IEnumerable<nint> affinityMasks,
				ThreadPriority priority = default,
				ILogger? logger = null) {
			if (affinityMasks is null) {
				throw new ArgumentNullException(nameof(affinityMasks));
			}

			var masks = affinityMasks.ToList();
			if (masks.Count is not >= 1) {
				throw new ArgumentException(null, nameof(affinityMasks));
			}

			_threadPoolId = Interlocked.Increment(ref ThreadPoolCount);
			_threadPriority = priority;
			_threads = new ComputationWorker[masks.Count];
			for (var i = 0; i < masks.Count; ++i) {
				_threads[i] = new(this, 0, masks[i]);
			}

			_logger = logger;
		}

		/// <inheritdoc cref="EnqueueWork{T}(Func{T}, CancellationToken)"/>
		public Task EnqueueWork(Action work, CancellationToken cancellationToken = default) {
			if (work is null) {
				throw new ArgumentNullException(nameof(work));
			}

			if (cancellationToken.IsCancellationRequested) {
				return Task.FromCanceled(cancellationToken);
			}

			if (_status != Running) {
				throw new InvalidOperationException();
			}

			AssertNotClosed();

			DelegateJob job = new(work, cancellationToken);
			EnqueueJob(job);
			return job.Task;
		}
		/// <summary>Enqueue a job to run on this thread pool.</summary>
		/// <typeparam name="T">
		///     The type of result expected from <paramref name="work"/>.
		/// </typeparam>
		/// <param name="work">The work to perform.</param>
		/// <param name="cancellationToken">
		///     A cancellation token that can be used to cancel the work if it has not yet started.
		/// </param>
		/// <returns>A task representing the status of the queued work.</returns>
		/// <exception cref="ArgumentNullException">
		///     if <paramref name="work"/> is null.
		/// </exception>
		public Task<T> EnqueueWork<T>(Func<T> work, CancellationToken cancellationToken = default) {
			if (work is null) {
				throw new ArgumentNullException(nameof(work));
			}

			if (cancellationToken.IsCancellationRequested) {
				return Task.FromCanceled<T>(cancellationToken);
			}

			AssertNotClosed();

			DelegateJob<T> job = new(work, cancellationToken);
			EnqueueJob(job);
			return job.Task;
		}

		private void EnqueueJob(Job job) {
			if (!TryEnqueueJob(job)) {
				Throw_ThreadPoolIsClosed();
			}
		}
		private bool TryEnqueueJob(Job job) {
			if (_status == Running) {
				lock (_jobQueue) {
					if (_status == Running) {
						_jobQueue.Enqueue(job);
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>Prevent new work from being queued.</summary>
		public void Close() {
			_status = Closed;
		}
		/// <summary>
		///     <inheritdoc cref="Close" path="/summary"/> Blocks the current thread until all
		///     remaining jobs are completed.
		/// </summary>
		public void CloseAndWait() {
			Close();
			for (var i = 0; i < _threads.Length; ++i) {
				_threads[i].Join();
			}
		}

		private void AssertNotClosed() {
			if (_status != Running) {
				Throw_ThreadPoolIsClosed();
			}
		}
		[DoesNotReturn]
		private static void Throw_ThreadPoolIsClosed() {
			throw new InvalidOperationException("thread pool is closed");
		}

		class ComputationWorker {
			private readonly ComputationThreadPool _pool;
			private readonly Thread _thread;
			private readonly int _index;
			private readonly string _name;
			private readonly nint _coreMask;

			public ComputationWorker(ComputationThreadPool pool, int index, nint coreMask) {
				if (pool is null) {
					throw new ArgumentNullException(nameof(pool));
				}
				if (index < 0) {
					throw new ArgumentOutOfRangeException(nameof(index), index, null);
				}

				_pool = pool;
				_thread = new(ThreadMain);
				_index = index;
				_name = $"ComputationThreadPool-{pool._threadPoolId}-Worker-{_index}";
				_coreMask = coreMask;

				_thread.Start(this);
			}

			public void Join() {
				_thread.Join();
			}

			private static void ThreadMain(object? arg) {
				var worker = (ComputationWorker)arg!;
				var pool = worker._pool;
				var jobQueue = pool._jobQueue;
				SpinWait wait = new();
				worker.ConfigureThread();
				var waitingForTasks = true;
				// keep going as long as the pool is not closed
				while (waitingForTasks) {
					// cache pool status before trying to dequeue
					waitingForTasks = pool._status == Running;
					// run jobs while they are available
					while (jobQueue.TryDequeue(out var job)) {
						worker.ConfigureThread();
						Interlocked.Increment(ref pool._runningWorkerCount);
						try {
							job.Run();
						}
						catch (Exception e) {
							pool._logger?.Log(LogLevel.Warning, e, "unexpected exception from job ");
						}
						finally {
							Interlocked.Decrement(ref pool._runningWorkerCount);
						}
						worker.ConfigureThread();
						wait.Reset();
					}
					wait.SpinOnce();
				}
			}
			private void ConfigureThread() {
				Thread.BeginThreadAffinity();
				Native.SetProcessorAffinity(_coreMask);
				_thread.IsBackground = true;
				_thread.Name = _name;
				_thread.Priority = _pool._threadPriority;
			}
		}

		#region Job Types
		abstract class Job {
			public abstract void Run();
		}
		sealed class DelegateJob : Job {
			public Task Task => _tcs.Task;

			private readonly TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
			private readonly Action _action;

			public DelegateJob(Action action, CancellationToken cancellationToken) {
				_action = action ?? throw new ArgumentNullException(nameof(action));
				cancellationToken.Register(
					arg => _tcs.TrySetCanceled((CancellationToken)arg!),
					cancellationToken);
			}

			public override void Run() {
				if (_tcs.Task.IsCompleted) {
					return;
				}

				try {
					_action();
					_tcs.TrySetResult();
				}
				catch (OperationCanceledException e) {
					_tcs.TrySetCanceled(e.CancellationToken);
				}
				catch (Exception e) {
					_tcs.TrySetException(e);
				}
			}
		}
		sealed class DelegateJob<T> : Job {
			public Task<T> Task => _tcs.Task;

			private readonly TaskCompletionSource<T> _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
			private readonly Func<T> _action;

			public DelegateJob(Func<T> action, CancellationToken cancellationToken) {
				_action = action ?? throw new ArgumentNullException(nameof(action));
				cancellationToken.Register(
					arg => _tcs.TrySetCanceled((CancellationToken)arg!),
					cancellationToken);
			}

			public override void Run() {
				if (_tcs.Task.IsCompleted) {
					return;
				}

				try {
					var result = _action();
					_tcs.TrySetResult(result);
				}
				catch (OperationCanceledException e) {
					_tcs.TrySetCanceled(e.CancellationToken);
				}
				catch (Exception e) {
					_tcs.TrySetException(e);
				}
			}
		}
		sealed class TaskJob : Job {
			private readonly Task _task;
			private readonly Scheduler _scheduler;

			public TaskJob(Task task, Scheduler scheduler) {
				_task = task ?? throw new ArgumentNullException(nameof(task));
				_scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
			}

			public override void Run() {
				if (!_scheduler.TryExecuteTask(_task)) {
					throw new InvalidOperationException("failed to run task");
				}
			}
		}
		#endregion

		private sealed class Scheduler : TaskScheduler {
			private readonly ComputationThreadPool _pool;

			public Scheduler(ComputationThreadPool pool) {
				_pool = pool ?? throw new ArgumentNullException(nameof(pool));
			}

			public new bool TryExecuteTask(Task task) {
				if (task is null) {
					throw new ArgumentNullException(nameof(task));
				}

				return base.TryExecuteTask(task);
			}

			protected override void QueueTask(Task task) {
				if (task is null) {
					throw new ArgumentNullException(nameof(task));
				}

				TaskJob job = new(task, this);
				if (!_pool.TryEnqueueJob(job)) {
					throw new InvalidOperationException();
				}
			}
			protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) {
				return false;
			}

			protected override IEnumerable<Task>? GetScheduledTasks() {
				throw new NotSupportedException();
			}
		}
	}
}
