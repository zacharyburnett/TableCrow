import pytest as pytest


def pytest_collection_modifyitems(config, items):
    keywordexpr = config.option.keyword
    markexpr = config.option.markexpr
    if keywordexpr or markexpr:
        return  # let pytest handle this

    skip_spatial = pytest.mark.skip(reason='spatial not selected')
    skip_postgres = pytest.mark.skip(reason='postgres not selected')
    for item in items:
        if 'spatial' in item.keywords:
            item.add_marker(skip_spatial)
        if 'postgres' in item.keywords:
            item.add_marker(skip_postgres)
