"""Prompt for an integer and report whether it is prime.

Primality uses a deterministic Miller-Rabin test, which is provably correct
(not probabilistic) for every n below MAX_N using the fixed witness set below.
"""
import logging
import re
import sys

logging.basicConfig(
    filename="prime.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# Testing against the first 13 primes as witnesses is proven deterministic for
# all n < 3,317,044,064,679,887,385,961,981 (Sorenson & Webster, 2015).
WITNESSES = (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41)
MAX_N = 3_317_044_064_679_887_385_961_981

# Plain ASCII integers only: optional sign, digits. Rejects "3.0", "1e3",
# "1_000", "0x1F", and non-ASCII digits that Python's int() would accept.
INT_PATTERN = re.compile(r"[+-]?[0-9]+")


def parse_int(text: str) -> int:
    """Convert user text to an int, raising ValueError for anything else."""
    s = text.strip()
    if not INT_PATTERN.fullmatch(s):
        raise ValueError(f"not an integer: {text!r}")
    return int(s)


def is_prime(n: int) -> bool:
    """Return True if n is prime. Raises for non-ints or n >= MAX_N."""
    if not isinstance(n, int) or isinstance(n, bool):
        raise TypeError("n must be an int")
    if n >= MAX_N:
        raise ValueError(f"n must be < {MAX_N} for a guaranteed-correct answer")
    if n < 2:
        return False
    # Small primes (also the witnesses) handled directly; catches their multiples.
    for p in WITNESSES:
        if n % p == 0:
            return n == p

    # Write n-1 as d * 2^s with d odd.
    d, s = n - 1, 0
    while d % 2 == 0:
        d //= 2
        s += 1

    # n is composite if any witness a proves it; otherwise it is prime.
    for a in WITNESSES:
        x = pow(a, d, n)
        if x in (1, n - 1):
            continue
        for _ in range(s - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def main() -> None:
    """Prompt repeatedly until a valid integer is entered, then report on it."""
    while True:
        try:
            raw = input("Enter an integer: ")
        except (EOFError, KeyboardInterrupt):
            print()
            log.info("input aborted")
            sys.exit(1)
        try:
            n = parse_int(raw)
            result = is_prime(n)
        except ValueError as e:
            print(f"Invalid input: {e}")
            log.warning("rejected input %r: %s", raw, e)
            continue
        print(f"{n} is {'prime' if result else 'not prime'}.")
        log.info("%d -> %s", n, "prime" if result else "not prime")
        return


if __name__ == "__main__":
    main()
