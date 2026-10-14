# Prime Checker

A command-line app that asks for an integer and tells you whether it is prime.

## Setup

This project uses the **pipenv** environment at the repository root (`ds3022/`).
Don't create a separate one in this folder.

```bash
cd ds3022            # repo root
pipenv install --dev # installs sympy (used by tests) and pytest
```

## Usage

```bash
cd ds3022/claude-demo
pipenv run python prime.py
```

```
Enter an integer: 3.5
Invalid input: not an integer: '3.5'
Enter an integer: 97
97 is prime.
```

- **Accepted:** plain integers with an optional sign and surrounding spaces, e.g. `42`, `-7`, `+13`, `007`.
- **Rejected (you'll be asked again):** decimals (`3.0`), scientific notation (`1e3`),
  hex (`0x1F`), separators (`1,000`, `1_000`), non-ASCII digits, text, and blank input.
- Zero, one, and negative numbers are reported as **not prime**.
- Supported range: up to 3,317,044,064,679,887,385,961,981 (about 3.3 × 10²⁴).
  Larger values are rejected rather than given an answer that might be wrong.
- Press `Ctrl+D` / `Ctrl+C` to quit.
- Every input and result is logged to `prime.log` in the current directory.

## How it works

`is_prime()` uses a **deterministic Miller–Rabin** test with the first 13 primes
(2 … 41) as witnesses. With that witness set the test is *proven* exact for every
n in the supported range (Sorenson & Webster, 2015). It is not a probabilistic
guess, and it handles 24-digit numbers instantly.

## Tests

```bash
cd ds3022/claude-demo
pipenv run pytest -v
```

`test_prime.py` checks the results against independent references:

| Check | What it shows |
|---|---|
| Every n from 0 to 200,000 vs. a Sieve of Eratosthenes | Exhaustive agreement on small inputs |
| π(10⁶) = 78,498 | Matches the known prime-counting value |
| 2,000 random n < 10¹⁰ vs. brute-force trial division | Agrees with the simplest possible method |
| 10,000 random n across the full range vs. `sympy.isprime` | Agrees with a trusted library on large inputs |
| Carmichael numbers and strong pseudoprimes | Numbers built to fool weaker tests are caught |
| Known large primes (2⁶¹−1, largest prime < 2⁶⁴, …) | Large primes are recognized |
| Input parsing and the interactive prompt | Non-integers are rejected and you're asked again |
