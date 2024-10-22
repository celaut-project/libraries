import threading


class Singleton(type):
    _instances = {}
    _locks = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            # Each class will have its own lock
            if cls not in cls._locks:
                Singleton._locks[cls] = threading.Lock()

            with Singleton._locks[cls]:
                # another thread could have created the instance
                # before we acquired the lock. So check that the
                # instance is still nonexistent.
                if cls not in cls._instances:
                    cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
