from __future__ import annotations

from liteseries import Rollback, close_ls, launch_ls, ls_cache, threadpool_shutdown_ls


def main() -> None:
    _ = (Rollback, close_ls, launch_ls, ls_cache, threadpool_shutdown_ls)


if __name__ == "__main__":
    main()
