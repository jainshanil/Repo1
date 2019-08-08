""" Shell-oriented resources for FSD project."""
from __future__ import unicode_literals
import subprocess, threading
import os, sys
import io

from Queue import Queue

# Set up shell logging
from fsd2.logging import getChild,FsdLogger,Stopwatch
shell_log = getChild(FsdLogger,"core.shell")

SHELL_ENCODING = 'utf-8'

class ShellIOWrapper(object):
    """ Wraps the file-like object returned by subprocess.Popen.
        This wrapper exists to facilitate closing multiple 
        file-like objects at once - see ._close_siblings(self)
    """

    def __init__(self,fileobj,*args,**kwargs):
        self._file = fileobj # File-like object
        self._siblings = kwargs.get('siblings',[]) # Other file-like elements to close
        self.buffered = kwargs.get('buffer',False)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
        self.close()

    def _close_siblings(self):
        """ Closes all file-like objects also related to 
            the object as listed in ._siblings. """
        for s in self._siblings:
            if s:
                s.close()
    
    # Reproductions of the file methods
    def close(self):
        self._close_siblings()
        self._file.close()

    def flush(self):
        self._file.flush()

    def __iter__(self):
        return self._file.__iter__()
    
    def next(self):
        return self._file.next()

    def read(self,size=-1):
        return self._file.read(size).decode(SHELL_ENCODING)

    def readline(self,size=-1):
        return self._file.readline(size).decode(SHELL_ENCODING)

    def write(self,value):
        self._file.write(value.encode(SHELL_ENCODING))
        if self.buffered:
            self.flush()

    def writelines(self,seq):
        self._file.writelines(seq)
        if self.buffered:
            self.flush()

    # Reproductions of file properties
    @property
    def closed(self):
        return self._file.closed

    @property
    def mode(self):
        return self._file.mode

    @property
    def name(self):
        return self._file.name

    @property
    def encoding(self):
        return self._file.encoding
        

class ShellIO(object):
    """ Generic class for working with the shell. Used primarily by ShellCommand, 
        but was split out so it can be used for streaming if the need arises."""
    
    def __init__(self,*args,**kwargs):
        """
            Accepts kwargs:
            - [opt] dir, feeds the 'cwd' argument to change working directory
            - executable, string that's the actual command to run
            - [opt] args, iterable of arguments ready for subprocess.Popen
            
        """
        # running values
        self.dir = kwargs.get('dir',None)
        self.executable = kwargs.get('executable',None)
        self.args = kwargs.get('args',[])

        # exec values
        self._process = None
        self._stdin = None
        self._stdout = None
        self._stderr = None

    def open_io(self,stdin=False,stdout=True,stderr=True):
        """ Runs the executable and argument in the command line
            and returns three file-like streams:
            
            1. stdin (None by default)
            2. stdout
            3. stderr
        
        """
        shell_log.debug("Opening IO with %s - executable: '%s'\targs: %s" % (','.join([a for a,b in filter(lambda a: a[1],[('stdin',stdin),('stdout',stdout),('stderr',stderr)])]),self.executable,self.args))
        # Check to make sure executable is string-like and args are a list
        if not (isinstance(self.executable,basestring) and hasattr(self.args,"__iter__")):
            raise ValueError("Executable must be text (was type: '%s') and arguments must be an iterable of text (was type: '%s')." % (type(self.executable),type(self.args)))

        # Process desired pipes
        redir_stdin = None
        if stdin:
            redir_stdin = subprocess.PIPE
        redir_stdout = None
        if stdout:
            redir_stdout = subprocess.PIPE
        redir_stderr = None
        if stderr:
            redir_stderr = subprocess.PIPE

        # Prepare arguments to Popen
        cmd_list = [self.executable] + self.args
        
        # Run Popen
        proc = subprocess.Popen(cmd_list,stdin=redir_stdin,stdout=redir_stdout,stderr=redir_stderr,cwd=self.dir)
        
        self._process = proc

        s_stdin = None
        s_stdout = None
        s_stderr = None

        if proc.stdin:
            s_stdin = ShellIOWrapper(proc.stdin,siblings=[proc.stdout,proc.stderr])

        if proc.stdout:
            s_stdout = ShellIOWrapper(proc.stdout,siblings=[proc.stdin,proc.stderr])

        if proc.stderr:
            s_stderr = ShellIOWrapper(proc.stderr,siblings=[proc.stdin,proc.stdout])
        
        return s_stdin,s_stdout,s_stderr

    def cleanup_io(self,kill=False):
        """ Does a basic cleanup on any open file objects or processes."""
        for obj in (self._stdin,self._stdout,self._stderr):
            if obj is not None:
                obj.close()
        if self._process is not None and self._process.poll():
            if not kill:
                self._process.wait()
            else:
                self._process.kill()
            

class ShellCommand(ShellIO):
    """
        Generic class for building and running shell commands.
        
        The persisted version of the syntax:
        1.  sc = ShellCommand(executable='ls',args=['-l'],timeout=90)
            results = sc.execute()
            
        or the non-persisted vesion:
        2.  results = ShellCommand(timeout=90).execute(executable='ls',args=['-l'])

    """

    def __init__(self,*args,**kwargs):
        """
            Accepts kwargs:
            - [opt] dir, feeds the 'cwd' argument to change working directory
            - executable, string that's the actual command to run
            - [opt] args, iterable of arguments ready for subprocess.Popen
            - timeout, number of seconds after which to terminate the command and raise a CommandTimeoutException
            
        """
        super(ShellCommand, self).__init__(self,*args,**kwargs)
        
        self.command_timeout = kwargs.get('timeout',None) # in seconds
        if self.command_timeout:
            self.command_timeout = int(self.command_timeout)
        
        self.encoding = kwargs.get('encoding','utf-8')
        
        # returned values
        self.output = None
        self.errors = None
        self.rtncode = None
        
        self.ignore_rtncode = kwargs.get('ignore_rtncode',False)

    def execute(self,return_output=True,*args,**kwargs):
        """
            Runs the shell command. May provide 'executable' and 'args' at
            runtime to use like ShellCommand().execute(...)
            
            Will return output of command execution unless return_output is set to False.
        """
        shell_log.debug("Executing %s \t executable=%s\targs=%s" % (self.__class__.__name__,self.executable,self.args))
        
        ignore_rtncode = kwargs.get('ignore_rtncode',self.ignore_rtncode)
            
        # Provide "express" executable and args if desired
        if not self.executable:
            self.executable = kwargs.get('executable')
        if not self.args:
            self.args = kwargs.get('args',[])

        def thread_runner(except_q):
            """ Runs the command in a new Thread with only a thread-safe Queue as the argument. """
            # devnull = open(os.devnull,'w')
            try:
                stdin,stdout,stderr = self.open_io(stdin=False,stdout=True,stderr=True)
                output,self.errors = self._process.communicate()
                self.output = output.decode(self.encoding)
            except Exception, e:
                shell_log.warn("Execution of '%s' resulted in an exception." % (self.executable))
                # Put complete exception on the exception Queue
                except_q.put(sys.exc_info())
                
        with Stopwatch() as exec_t:
            # Set up and start Thread for command (threading is used to provide timeout control)
            exception_queue = Queue()
            cmd_thread = threading.Thread(target=thread_runner,args=(exception_queue,))
            cmd_thread.start()

            # Wait for thread to complete OR when it reaches self.command_timeout
            cmd_thread.join(self.command_timeout)
            
            # Check if thread was still alive when .join(timeout) killed it
            if cmd_thread.is_alive():
                self._process.terminate()
                cmd_thread.join()
                # Raise an exception if we timed out the command
                error_msg = "Execution of '%s' timed out at %d seconds.\n%s" % (self.executable,self.command_timeout,self.errors)
                shell_log.error(error_msg)
                raise CommandTimeoutException(error_msg)

            # Retrieve thread exception, if any, and raise as original
            if not exception_queue.empty():
                thread_exception = exception_queue.get_nowait()
                raise thread_exception[1],None,thread_exception[2]
            
            # Retain output from process 
            self.rtncode = self._process.returncode

            # Ensure all filelike objects and process are closed out
            self.cleanup_io()

        shell_log.debug("Execution of '%s' took %.2f sec." % (self.executable,exec_t.elapsed))
        
        # Checks returncode of completed command
        if self.rtncode != 0:
            error_msg = "Execution of '%s' resulted in non-zero return code '%d'.\n%s" % (self.executable,self.rtncode,self.errors)
            if ignore_rtncode:
                shell_log.warn(error_msg)
            else:
                shell_log.error(error_msg)
                raise ShellCommandException(error_msg)
        
        # Optionally returns the command results (from stdout)
        if return_output:
            return self.output
        
class ShellCommandException(Exception):
    """ Used to throw errors associated with ShellCommand execution."""
    pass

class CommandTimeoutException(ShellCommandException):
    """ Used to signify a timeout on running a shell command."""
    pass
