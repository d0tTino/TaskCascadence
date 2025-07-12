# Cascadence

Cascadence aims to provide a flexible framework for orchestrating complex, multi-step tasks. The project is inspired by the Product Requirements Document (PRD), which outlines features such as:

- Graph-based task definitions for clear dependency management.
- Plugin architecture allowing custom processors and extensions.
- Configurable execution engine supporting synchronous and asynchronous flows.
- Built-in instrumentation and monitoring.

This repository lays the groundwork for the Python package implementation.

## External Wrappers

Wrappers written in other languages (for example Rust or Go) can integrate with
Cascadence by exposing HTTP endpoints that follow the CronyxServer API. At
minimum the wrapper should implement:

* `GET /tasks` - returns a JSON array of available tasks.
* `GET /tasks/<id>` - returns a JSON description of a task.

The :class:`CronyxServerLoader` plugin can then load these definitions at
runtime.
