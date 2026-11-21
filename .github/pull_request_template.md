## Summary

<!-- What changed and which case or signal it affects. -->

## Schema diff

<!-- New or changed tables and columns. Write "none" if there is no schema change. -->

## Eval results

<!-- Output of `python -m signal_service.cli eval`. Include row count, topic accuracy, and pass/fail. -->

## Cost estimate

<!-- Estimated Grok spend for the change. Write "none" for changes that do not call the model. -->

## Test plan

- [ ] `python -m pytest`
- [ ] `python -m signal_service.cli eval`
- [ ] `python -m signal_service.cli run-demo` on the seeded warehouse
