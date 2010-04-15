"""
Provides ``terminate(popen)``, an equivalent of ``popen.terminate()`` in
Python 2.6+.
"""


from sys import platform
from subprocess import Popen

if callable(getattr(Popen, 'terminate', None)):
    # Python 2.6+
    terminate = Popen.terminate

elif platform == 'win32':
    # Python 2.5- on Windows fallback
    from win32process import TerminateProcess
    def terminate(popen):
        TerminateProcess(popen._handle, -1)

else:
    # Python 2.5- on  *NIX fallback
    from os import kill
    from signal import SIGTERM
    def terminate(popen):
        kill(popen.pid, SIGTERM)
