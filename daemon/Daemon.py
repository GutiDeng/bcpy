import sys, os, time, atexit, signal
 
class Daemon:
    def __init__(self):
        self._cli_cmd = None
        self._cli_pname = '__UNNAMED__'
        self._cli_daemonize= False
        self._cli_args = {}
        
        self._pid_path_dir = '/tmp/bcpy/daemon'
        self._pid = None
        self._runnable = None
        self._atexits = []
        
        self._daemonized = False
        self._learned = False
    
    @property
    def runnable(self):
        return self._runnable
    @runnable.setter
    def runnable(self, f):
        self._runnable = f
    
    def register_atexit(self, f):
        self._atexits.append(f)
    
    def learn_cli(self):
        self._learned = True
        for arg in sys.argv:
            fs = arg.split('=')
            if len(fs) != 2:
                continue
            k, v = fs
            
            if k == '--bcpy-cmd':
                self._cli_cmd = v
            elif k == '--bcpy-cmd':
                self._cli_pname = v
            elif k == '--bcpy-daemonize':
                self._cli_daemonize = True if v.lower() == 'true' else False
            else:
                if k.startswith('--'):
                    k = k[2:]
                self._cli_args.update([(k, v)])
    
    def take_over(self):
        if not self._learned:
            self.learn_cli()
        
        if self._cli_pname == '__UNNAMED__':
            self._cli_pname = os.path.basename(sys.argv[0])
            sys.stderr.write('[WARNING] Using filename as pname: %s\n' % self._cli_pname)
        
        if self._cli_cmd == 'start':
            self.start()
        elif self._cli_cmd == 'stop':
            self.stop()
        elif self._cli_cmd == 'restart':
            self.restart()
        else:
            sys.stderr.write('[ERROR] Mandatory: --bcpy-cmd=start|stop|restart\n')
            sys.exit(-1)
    
    
    @property
    def pid_path_dir(self):
        return self._pid_path_dir
    @property
    def pid_path(self):
        return os.path.join(self.pid_path_dir,  self._cli_pname)
    
    def write_pid(self):
        d = os.path.dirname(self.pid_path)
        if not os.access(d, os.W_OK):
            os.makedirs(d)
        with open(self.pid_path, 'w') as f:
            f.write('%s\n' % self._pid)
    
    def remove_pid(self):
        if os.access(self.pid_path, os.W_OK):
            os.remove(self.pid_path)
            return 0
        return -1
    
    def daemonize_if_necessary(self):
        if self._cli_daemonize and not self._daemonized:
            self.daemonize()
    
    def daemonize(self):
        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)
        
        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)
        
        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file('/dev/null', 'r')
        so = file('/dev/null', 'a+')
        se = file('/dev/null', 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())
        
        self._daemonized = True
    
    
    def start(self):
        try:
            with open(self.pid_path) as f:
                pid = int(f.read().strip())
        except IOError:
            pid = None
        
        if pid:
            message = "[ERROR] Pid file %s exists.\n"
            sys.stderr.write(message % self.pid_path)
            sys.exit(1)
        
        if self._cli_daemonize and not self._daemonized:
            self.daemonize()
        
        self._pid = os.getpid()
        self.write_pid()
        atexit.register(self.remove_pid)
        [atexit.register(f) for f in self._atexits]
        
        self.runnable(**self._cli_args)

    def stop(self):
        try:
            with open(self.pid_path) as f:
                pid = int(f.read().strip())
        except IOError:
            pid = None
        
        if not pid:
            message = "[WARNING] Pid file %s does not exist.\n"
            sys.stderr.write(message % self.pid_path)
            return 
        
        try:
            for i in range(10):
                sys.stderr.write('[INFO] Sending SIGTERM, %s times... \n' % i)
                os.kill(pid, signal.SIGTERM)
                time.sleep(1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                self.remove_pid()
                sys.stderr.write('[INFO] Target process no longer exists. \n')
                return 0
            else:
                sys.stderr.write(err)
                return -1
    
    def restart(self):
        self.stop() == 0 and self.start()
    

if __name__ == '__main__':
    class Test:
        def run(self, **kwargs):
            print 'runnable with arguments:', ', '.join(['%s=%s'%(k, v) for k, v in kwargs.items()])
            print 'sleep(3)'
            time.sleep(3)
        def cleanup(self):
            print 'cleanup'
    
    t = Test()
    d = Daemon()
    d.runnable = t.run
    d.register_atexit(t.cleanup)
    d.take_over()

