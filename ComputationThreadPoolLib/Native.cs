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

using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using System.Security;

namespace ComputationThreadPoolLib {
	internal class Native {
		const string Kernel32 = "kernel32.dll";

		/// <summary>Sets the affinity mask of the current <em>native</em> thread.</summary>
		/// <remarks>
		///     Managed threads are not always guaranteed to stay on the same native thread. See
		///     <see cref="Thread.BeginThreadAffinity"/> for more information.
		/// </remarks>
		public static void SetProcessorAffinity(IntPtr coreMask) {
			int threadId = GetCurrentThreadId();
			SafeThreadHandle? handle = null;
			try {
				handle = OpenThread(0x60, false, threadId);
				if (SetThreadAffinityMask(handle, coreMask) == IntPtr.Zero) {
					throw new Exception("Failed to set processor affinity for thread");
				}
			}
			finally {
				if (handle != null) {
					handle.Close();
				}
			}
		}

		/// <summary>Represents a wrapper class for a thread handle.</summary>
		[SuppressUnmanagedCodeSecurity]
		public class SafeThreadHandle : SafeHandleZeroOrMinusOneIsInvalid {
			/// <summary>
			///     Initializes a new instance of the <see cref="SafeThreadHandle"/> class.
			/// </summary>
			public SafeThreadHandle() : base(true) { }

			protected override bool ReleaseHandle() {
				return CloseHandle(handle);
			}
		}

		[DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
		public static extern IntPtr SetThreadAffinityMask(SafeThreadHandle handle, IntPtr mask);
		[DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true, ExactSpelling = true)]
		public static extern bool CloseHandle(IntPtr handle);
		[DllImport(Kernel32)]
		public static extern int GetCurrentThreadId();
		[DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
		public static extern SafeThreadHandle OpenThread(int access, bool inherit, int threadId);
	}
}
