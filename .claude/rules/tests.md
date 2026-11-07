---
paths:
  - "tests/**"
---

# Test rules

* xUnit v3 (`[Fact]`, `[Theory]`), Shouldly assertions (`x.ShouldBe(y)`), NSubstitute for doubles (D-31). Test names: `Method_Condition_Expectation` or a plain sentence in PascalCase.
* Unit tests have no external dependencies: use `FakeTimeProvider` for time, the in-memory transport (from S08) for messaging, `TestContext.Current.CancellationToken` for cancellation.
* Integration tests that need Valkey, MQTT or other services run inside the compose harness (`compose.test.yaml`, D-15), not against services on the host. Mark them so they can be filtered (trait `Category=Integration`).
* Python tests on `python-archive` (`git show python-archive:tests/...`) are the behavioural specification; port the scenario, not the code, and use the new wire format (D-2).
* Every bug fix gets a test that fails without the fix.
* No sleeps for synchronisation; wait on a signal or advance the fake clock.
