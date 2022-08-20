# src/fogdb/dbs/cassy.py
"""Module fo dynamically creating hardcoded data models."""

import importlib
import logging
from dataclasses import dataclass
from pathlib import Path
from shutil import copy2
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class MetaModel:
    """Dynamically create a table model.

    Parameters
    ----------
    name: str
        String specifying the table name. Will be the name of the CQL table
        for this model.
    primary_keys: dict
        Dictionairy with primary keys as key and respective column data type as
        value as in ::

            primary_keys = {"my_primary_key": "Text"}

    clustering_keys: dict
        Dictionairy with clustering keys as keys and
        ``("Column Data Type", "clustering_order")`` tuple specifying column
        data type and clustering order as in::

            clustering_keys = {
                "my_clustering_key": ("Text", "ASC"),
                "my_other_clustering_key": ("Text", "DESC"),
            }
    columns: str, container
        Dictionairy with column names as key and respective column data type as
        value as in ::

            columns = {
                "my_attribute": "Text",
                "my_other_attribute": "Int",
                "my_third_attribute": "Blob",
            }

        See the `DataStax documentation
        <https://docs.datastax.com/en/developer/python-dse-driver/2.11/api/dse/cqlengine/columns/#column-types>`_
        for available datatypes.

    Examples
    --------
    Default use case::

        my_meta_model = MetaModel(
            name="CrawfordCommonFruitingTrees",
            primary_keys={"Latin": "Text"},
            clustering_keys={
                "English": ("Text", "ASC"),
                "German": ("Text", "ASC"),
            },
            columns={"USDA_Hardiness": "Integer"},
        )

    See Also
    --------
    `Cassandra Driver Data Model
    <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_
    """

    name: str
    primary_keys: dict
    clustering_keys: dict
    columns: dict


def _write_model_lines(meta_model, path):
    lines_to_write = [
        f"# {path}\n",
        "# automatically created using cassy\n",
        "from cassandra.cqlengine import columns\n",
        "from cassandra.cqlengine.models import Model\n\n\n",
        f"class {meta_model.name}(Model):\n",
    ]
    for column_name, column_data_type in meta_model.primary_keys.items():
        line = f"    {column_name} = columns.{column_data_type}(primary_key=True)\n"
        lines_to_write.append(line)

    for column_name, column_data_type in meta_model.clustering_keys.items():
        # pylint: disable=line-too-long
        line = f"    {column_name} = columns.{column_data_type[0]}(primary_key=True, clustering_order='{column_data_type[1]}')\n"
        lines_to_write.append(line)
        # pylint: enable=line-too-long

    lines_to_write.append("\n    # Regular Table Columns:\n")

    for column_name, column_data_type in meta_model.columns.items():
        line = f"    {column_name} = columns.{column_data_type}()\n"
        lines_to_write.append(line)

    return lines_to_write


def _handle_path(path, overwrite=False, backup=True):

    if str(path).startswith("~"):  # pragma: no cover
        # coverage is excluded here, cause this part definetly gets
        # executed and tested. I might be overlooking something,
        # tests/db_api/test_model.test_create_model_design_case
        # uses "~" as first path element, so Im not sure, whats happening
        output_path = Path(path).expanduser()
    else:
        output_path = Path(path).resolve()

    if output_path.is_file():
        logger.debug("Existing file detected at %s", output_path)

        if not overwrite:
            error_msg = f"Existing file at {output_path}\n"
            fix_msg = "Set 'overwrite=True' or change filepath"
            raise FileExistsError(error_msg + fix_msg)
        if backup:
            # create uid for not overwriting existing backups
            uid = str(uuid4())
            otp = output_path  # shortcut the output_path for one liner below
            parent, name, extension = otp.parent, otp.stem, "".join(otp.suffixes)
            backup_name = "_backup_".join([name, uid])
            backup_path = Path(parent) / "".join([backup_name, extension])

            logger.debug("Copying exsting file to  %s", backup_path)
            copy2(src=otp, dst=backup_path)

    return output_path


def create_data_model(meta_model, path, overwrite=False, backup=True):
    """Dynamically create a table model.

    Creates a `Datastax Cassandry Cqlenine Data Model
    <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_

    Parameters
    ----------
    meta_model: :class:`MetaModel`
        dataclass describing the model data.
    path: pathlib.Path, str
        Path or string specifying the folder the hardcoded
        module will be located.
    overwrite: bool, default=False
        Boolean indicating whether the hardcoded model data file should be
        overwritten. If :paramref:`~create_data_model.backup` is True,
        exsting file will be renamed to
        ``existing-name_backup_hash(timestamp).py``
    backup: bool, default=True
        Boolean indicating whether the hardcoded model data file should be
        kept as backup in case of overwriting. Exsting file will be renamed to
        ``existing-name_backup_hash(timestamp).py``

    Returns
    -------
    pathlib.Path
        Path the hardcoded data model file was created at.

    Examples
    --------
    Default use case::

        my_meta_model = MetaModel(
            name="CrawfordCommonFruitingTrees",
            primary_keys={"Latin": "Text"},
            clustering_keys={
                "English": ("Text", "ASC"),
                "German": ("Text", "ASC"),
            },
            columns={"USDA_Hardiness": "Integer"},
        )

        path = create_data_model(
            meta_model=my_meta_model,
            path=os.path.join("~", ".fogd.d", "my_model.py"),
            overwrite=True,
        )
    """
    lines_to_write = _write_model_lines(meta_model, path)
    output_path = _handle_path(path, overwrite, backup)

    with open(output_path, "w+", encoding="utf8") as file_handle:
        file_handle.writelines(lines_to_write)

    return output_path


def retrieve_data_model(path, model_name):
    """Retrieve previously hardcoded data model.

    Parameters
    ----------
    path: pathlib.Path, str
        Path or string specifying the folder the hardcoded
        module will be located.
    model_name: str
        String specifying the :paramref:`MetaModel.name`.

    Returns
    -------
    DataModel
        `Datastax Cassandry Cqlenine Data Model
        <https://docs.datastax.com/en/developer/python-driver/3.25/api/cassandra/cqlengine/models/>`_
        previously hardcoded in :paramref:`~retrieve_data_model.path`.

    Examples
    --------
    Default use case::

        model = retrieve_data_model(
            path=os.path.join("~", ".fogd.d", "pytest_home_model.py"),
            model_name="CrawfordCommonFruitingTrees",
        )
    """
    input_path = Path(path).expanduser()

    spec = importlib.util.spec_from_file_location(
        input_path.stem,
        str(input_path),
    )
    module = importlib.util.module_from_spec(spec)

    # sys.modules[module_name] = module
    spec.loader.exec_module(module)

    model = getattr(module, model_name)
    model.__table_name__ = model_name
    model.__table_name_case_sensitive__ = True

    return model
