import importlib
import inspect
import pkgutil


def _defined_public_class_names(package_module) -> set[str]:
    names: set[str] = set()
    for module_info in pkgutil.iter_modules(package_module.__path__):
        if module_info.ispkg or module_info.name.startswith("_"):
            continue
        module = importlib.import_module(
            f"{package_module.__name__}.{module_info.name}"
        )
        for name, value in vars(module).items():
            if name.startswith("_"):
                continue
            if inspect.isclass(value) and value.__module__ == module.__name__:
                names.add(name)
    return names


class TestProcessorsPackageExports:
    def test_known_imports_work_from_package(self):
        import microdcs.processors as processors

        assert (
            processors.GreetingsCloudEventProcessor.__name__
            == "GreetingsCloudEventProcessor"
        )
        assert (
            processors.MachineryJobsCloudEventProcessor.__name__
            == "MachineryJobsCloudEventProcessor"
        )
        assert processors.JobAcceptanceConfig.__name__ == "JobAcceptanceConfig"

    def test_all_submodule_defined_classes_are_exported(self):
        import microdcs.processors as processors

        expected = _defined_public_class_names(processors)
        assert expected
        assert expected.issubset(set(processors.__all__))
        for name in expected:
            assert hasattr(processors, name)


class TestModelsPackageExports:
    def test_known_imports_work_from_package(self):
        import microdcs.models as models

        assert models.AbortCall.__name__ == "AbortCall"
        assert models.Hello.__name__ == "Hello"
        assert models.JobOrderControlExt.__name__ == "JobOrderControlExt"
        assert models.GreetingsDataClassMixin.__name__ == "GreetingsDataClassMixin"

    def test_all_submodule_defined_classes_are_exported(self):
        import microdcs.models as models

        expected = _defined_public_class_names(models)
        assert expected
        for name in expected:
            assert hasattr(models, name)


class TestPublishersPackageExports:
    def test_known_imports_work_from_package(self):
        import microdcs.publishers as publishers

        assert publishers.JobOrderPublisher.__name__ == "JobOrderPublisher"

    def test_all_submodule_defined_classes_are_exported(self):
        import microdcs.publishers as publishers

        expected = _defined_public_class_names(publishers)
        assert expected
        assert expected.issubset(set(publishers.__all__))
        for name in expected:
            assert hasattr(publishers, name)
