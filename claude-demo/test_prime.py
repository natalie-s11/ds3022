"""Test suite for prime.py.

Correctness is checked against independent references: a brute-force trial
division oracle, a Sieve of Eratosthenes, and sympy.isprime.
"""
import random

import pytest
import sympy

from prime import MAX_N, is_prime, main, parse_int


def trial_division(n: int) -> bool:
    """Slow but obviously-correct reference implementation."""
    if n < 2:
        return False
    i = 2
    while i * i <= n:
        if n % i == 0:
            return False
        i += 1
    return True


def sieve(limit: int) -> list[bool]:
    """Sieve of Eratosthenes: flags[i] is True iff i is prime, for 0..limit."""
    flags = [False, False] + [True] * (limit - 1)
    for i in range(2, int(limit**0.5) + 1):
        if flags[i]:
            flags[i * i :: i] = [False] * len(flags[i * i :: i])
    return flags


# ---------- is_prime correctness ----------

def test_exhaustive_against_sieve():
    """Every integer from 0 to 200,000 matches the sieve."""
    flags = sieve(200_000)
    mismatches = [n for n, p in enumerate(flags) if is_prime(n) != p]
    assert mismatches == []


def test_first_primes_exact():
    assert [n for n in range(100) if is_prime(n)] == [
        2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47,
        53, 59, 61, 67, 71, 73, 79, 83, 89, 97,
    ]


def test_prime_counting_function():
    """pi(10^6) = 78,498 is a well-known value."""
    assert sum(sieve(10**6)) == 78_498
    assert sum(is_prime(n) for n in range(10**6 + 1)) == 78_498


@pytest.mark.parametrize("n", [0, 1, -1, -2, -7, -97, -(10**20)])
def test_non_positive_and_one_are_not_prime(n):
    assert is_prime(n) is False


@pytest.mark.parametrize("n", [
    561, 1105, 1729, 2465, 2821, 6601, 8911, 41041, 825265,  # Carmichael numbers
    2047, 3277, 4033, 4681, 8321,                            # strong pseudoprimes base 2
    3215031751,                                              # strong psp to bases 2,3,5,7
    3825123056546413051,                                     # strong psp to bases 2..23
    318665857834031151167461,                                # strong psp to bases 2..37
])
def test_pseudoprimes_are_composite(n):
    """Numbers specifically constructed to fool weaker primality tests."""
    assert is_prime(n) is False
    assert sympy.isprime(n) is False


@pytest.mark.parametrize("n", [
    2**31 - 1, 2**61 - 1,                  # Mersenne primes
    1_000_000_007, 998_244_353,            # well-known primes
    999_999_999_989,                       # largest prime below 10^12
    18_446_744_073_709_551_557,            # largest prime below 2^64
])
def test_known_large_primes(n):
    assert is_prime(n) is True


@pytest.mark.parametrize("p,q", [(65537, 65539), (999_983, 1_000_003), (2**31 - 1, 1_000_000_007)])
def test_semiprimes_are_composite(p, q):
    """Products of two large primes have no small factors to give them away."""
    assert is_prime(p * q) is False


@pytest.mark.parametrize("p", [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41])
def test_witness_primes_and_their_squares(p):
    """Edge case: the witness bases themselves are prime, their squares aren't."""
    assert is_prime(p) is True
    assert is_prime(p * p) is False


def test_random_large_against_sympy():
    """10,000 random values across the full supported range agree with sympy."""
    rng = random.Random(3022)
    for _ in range(10_000):
        n = rng.randrange(MAX_N)
        assert is_prime(n) == sympy.isprime(n), n


def test_random_mid_against_trial_division():
    """Random values up to 10^10 agree with brute force."""
    rng = random.Random(42)
    for _ in range(2_000):
        n = rng.randrange(10**10)
        assert is_prime(n) == trial_division(n), n


def test_rejects_out_of_range_and_non_int():
    with pytest.raises(ValueError):
        is_prime(MAX_N)
    for bad in (7.0, "7", True, None):
        with pytest.raises(TypeError):
            is_prime(bad)


# ---------- input parsing ----------

@pytest.mark.parametrize("text,expected", [
    ("7", 7), ("  42  ", 42), ("-13", -13), ("+5", 5), ("0", 0), ("007", 7),
    ("123456789012345678901234567890", 123456789012345678901234567890),
])
def test_parse_valid(text, expected):
    assert parse_int(text) == expected


@pytest.mark.parametrize("text", [
    "", "   ", "abc", "3.0", "3.5", "1e3", "1_000", "0x1F", "1,000",
    "--5", "5-", "7 7", "inf", "nan", "٣", "１２",  # Arabic-Indic / full-width digits
])
def test_parse_rejects_non_integers(text):
    with pytest.raises(ValueError):
        parse_int(text)


# ---------- interactive main() ----------

def run_main(monkeypatch, capsys, inputs):
    """Feed a sequence of lines to main() and return what it printed."""
    feed = iter(inputs)
    monkeypatch.setattr("builtins.input", lambda _prompt: next(feed))
    main()
    return capsys.readouterr().out


def test_main_prime(monkeypatch, capsys):
    assert "97 is prime." in run_main(monkeypatch, capsys, ["97"])


def test_main_not_prime(monkeypatch, capsys):
    assert "91 is not prime." in run_main(monkeypatch, capsys, ["91"])


def test_main_reprompts_on_bad_input(monkeypatch, capsys):
    out = run_main(monkeypatch, capsys, ["hello", "2.5", "17"])
    assert out.count("Invalid input") == 2
    assert "17 is prime." in out


def test_main_exits_on_eof(monkeypatch):
    def eof(_prompt):
        raise EOFError
    monkeypatch.setattr("builtins.input", eof)
    with pytest.raises(SystemExit):
        main()
