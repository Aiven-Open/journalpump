def make_cursor(*, s: int = 1, i: int = 1, b: int = 1, m: int = 1, t: int = 1) -> str:
    return f"s={s:x};i={i:x};b={b:x};m={m:x};t={t:x};x=abc"
