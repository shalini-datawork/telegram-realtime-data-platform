import fcntl
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def checkpoint_lock(checkpoint_path):

    checkpoint_path = Path(checkpoint_path)

    lock_path = checkpoint_path.with_suffix(
        checkpoint_path.suffix + ".lock"
    )

    lock_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with lock_path.open("a+") as lock_file:

        fcntl.flock(
            lock_file.fileno(),
            fcntl.LOCK_EX,
        )

        try:
            yield

        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN,
            )
