from typing import Literal, Optional

import polars as pl


def _to(pd_dt, pl_dt, engine):
    return pd_dt if "engine" == "pandas" else pl_dt


def _cast_to(
    datatype: str, format: Optional[tuple], engine: Literal["pandas", "polars"]
):
    match datatype.lower():
        case "text" | "url":
            return _to("string", pl.String, engine)
        case "calendar date":
            if not format:
                # raise ValueError(
                #     "Invalid casting to datetime without format information"
                # )
                return _to("datetime", pl.Datetime, engine)
            elif format and "view" not in format:
                return _to("datatime", pl.Date, engine)

            match format["view"]:
                # FIX: Date/Datetime
                case "date" | "date_ymd":
                    return _to("datetime %Y-%m-%d", pl.Datetime, engine)
                case "date_my":
                    return _to("datetime %Y-%m", pl.Datetime, engine)
                case "date_y":
                    return _to("datetime %Y", pl.Datetime, engine)
                case "date_time" | "default_date_time" | "iso_8601_date":
                    return _to("datetime", pl.Datetime, engine)
                case _:
                    raise ValueError(
                        f"Unexpected value in datetime casting: {datatype}, {format}"
                    )
        case "number":
            if not format:
                # raise ValueError("Invalid casting to number without format information")
                return _to("float", pl.Float32, engine)
            elif format and "noCommas" not in format:
                return _to("integer", pl.Int32, engine)

            match format["noCommas"]:
                case "true":
                    return _to("integer", pl.Int32, engine)
                case "false":
                    return _to("float", pl.Float32, engine)
        case _:
            return _to("string", pl.String, engine)


def cast_socrata_types(
    datatypes: list[dict], engine: Literal["pandas", "polars"]
) -> dict:
    return {h["name"]: _cast_to(h["data_type"], h["format"], engine) for h in datatypes}
