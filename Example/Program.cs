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

using ComputationThreadPoolLib;
using System.Runtime.InteropServices;
using System.Security;

namespace Example {
	public class Program {
		static void Main() {
			// NOTE Real program should be SMT-aware when selecting masks
			ComputationThreadPool pool = new(
				Enumerable.Range(0, Environment.ProcessorCount)
					.Select(x => (nint)(1 << x)));

			// Use task scheduling to enqueue many jobs on the queue
			var workGeneratorTask = Task.Factory.StartNew(
				() => {
					List<Task> tasks = new();
					for (var i = 0; i < (int)(Environment.ProcessorCount * 2.5); ++i) {
						var iCopy = i;
						Task.Factory.StartNew(() => {
							Console.WriteLine($"{iCopy,2} {GetCurrentProcessorNumber(),2}");
							Thread.SpinWait(100_000_000);
						});
					}
				},
				default,
				default,
				pool.TaskScheduler);

			// wait for jobs to enqueue and finish
			workGeneratorTask.Wait();
			pool.CloseAndWait();
			Console.WriteLine("jobs completed!");
		}

		[DllImport("kernel32.dll")]
		[SuppressUnmanagedCodeSecurity]
		static extern int GetCurrentProcessorNumber();
	}
}
