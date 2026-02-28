```markdown
# AGENTS.md - AI Coding Agent Guidelines

These guidelines outline the principles and rules for development of AI coding agents within this repository. Adherence to these principles is crucial for maintaining a sustainable, maintainable, and high-quality codebase.

## 1. DRY (Don't Repeat Yourself)

- All code snippets, algorithms, and logic should be encapsulated within reusable functions, classes, or modules.
- Avoid duplicating logic across multiple places.
- When a solution is found once, it should be readily available for reuse.

## 2. KISS (Keep It Simple, Stupid)

- Code should be as concise and easy to understand as possible.
- Prioritize readability and clarity over complex or verbose solutions.
- Resist unnecessary abstractions.  Keep the core logic simple.
- Favor straightforward implementations.

## 3. SOLID Principles

- **Single Responsibility Principle:** Each class/module should have one, and only one, well-defined responsibility.
- **Open/Closed Principle:**  Code should be extensible without modifying existing code.  New functionality should be added as separate, independent components.
- **Liskov Substitution Principle:**  Subclasses should be substitutable for their base classes without altering the correctness of the program.
- **Interface Segregation Principle:**  Clients should not be forced to bound to methods they don't use.
- **Dependency Inversion Principle:**  High-level modules should not depend on low-level modules.  Interfaces should dictate dependencies.

## 4. YAGNI (You Aren't Gonna Need It)

- Avoid implementing functionality that is not currently required.
- Only add features or code that is explicitly intended to be used.
- Resist the temptation to implement “just in case” solutions.

## 5. Code Structure & Formatting

- **File Size Limit:** Each file must be no more than 180 lines of code.
- **Comments:**  Use concise and informative comments where necessary to explain complex logic or rationale.  Comments should not introduce new code.
- **Naming Conventions:**  Follow consistent naming conventions (e.g., snake_case, camelCase).
- **Indentation:**  Use 2 spaces for indentation.  Don't use tabs.
- **Line Length:** Limit line lengths to 120 characters.

## 6. Test Coverage & Production

- **Mocking Only:**  All tests must be driven by mocks and stubs, *not* by actual implementations.
- **Test-Driven Development:**  Prioritize writing tests *before* writing code.
- **Unit Tests:**  Focus on unit tests that verify individual functions or classes in isolation.
- **Test Coverage Percentage:**  Aim for a minimum test coverage of 80% or higher.  Automated test suite is required.
- **Test Case Design:** Tests should cover all core functionality, edge cases and boundary conditions as specified.


## 7. Development Process

- **Code Review:** All changes should undergo peer code review before merging.
- **Documentation:**  Provide clear and concise documentation for each function or class, detailing its purpose and inputs/outputs.
- **Version Control:** Use Git for version control.  Maintain a clear commit history.
- **Continuous Integration (CI):** Implement a CI pipeline to automatically test code changes.
- **Continuous Deployment (CD):** Automate the deployment process as part of the CI pipeline.


## 8. Specific Considerations for AGENTS.md

- **Agent Initialization:**  All agent code must clearly define its initialization process.
- **Data Management:**  Implement robust data management strategies.
- **Communication Protocol:** Define a consistent communication protocol for agent interaction.
- **Error Handling:**  Provide informative error handling and logging.
- **Configuration:** Utilize a configuration mechanism to manage agent parameters.



## 9.  Resource Allocation

-  Allocate sufficient time for testing and documentation.
-  Prioritize code quality and maintainability during development.

These guidelines are intended as a starting point and may be adjusted as needed based on the evolution of the project.  Ongoing review and refinement are vital.
```