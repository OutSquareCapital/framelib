"""Tests for internals in framelib."""

from enum import StrEnum, auto

import pytest

import framelib as fl


def _check_dict(val: object) -> None:
    with pytest.raises(AttributeError):
        _ = val.__dict__


def test_slots() -> None:
    """Check if all instanciable classes have __slots__ defined."""

    class MyEnum(StrEnum):
        THIS = auto()
        THAT = auto()

    class MySchema(fl.Schema):  # Schema are not instanciated, so no slots needed
        date: fl.Date = fl.Date()
        name: fl.String = fl.String()
        category: fl.String = fl.String()
        value: fl.Float32 = fl.Float32()
        key: fl.Int32 = fl.Int32()
        enumeration: fl.Enum = fl.Enum(MyEnum)
        tensor: fl.Array = fl.Array(inner=fl.Float32(), shape=(3, 4))
        timestamp: fl.Datetime = fl.Datetime()
        struct: fl.Struct = fl.Struct({
            "sub_id": fl.Int64(),
            "sub_value": fl.Float64(),
        })
        dec: fl.Decimal = fl.Decimal(precision=10, scale=2)

    class MyDb(fl.DataBase):  # DataBase, being both a layout AND entry, can't use slots
        table1: fl.Table = fl.Table(MySchema)

    class MyFolder(fl.Folder):
        file1: fl.NDJson = fl.NDJson(MySchema)
        file2: fl.Parquet = fl.Parquet(MySchema)
        file3: fl.CSV = fl.CSV(MySchema)
        file4: fl.ParquetPartitioned = fl.ParquetPartitioned(
            partition_by="date", schema=MySchema
        )
        file5: fl.Json = fl.Json(MySchema)

    MySchema.entries().values().iter().for_each(_check_dict)
    _check_dict(MyDb.table1)
    MyFolder.entries().values().iter().for_each(_check_dict)
