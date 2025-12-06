from typing import Literal, Optional
import polars as pl


def _to(pl_dt, pd_dt, engine):
    return pl_dt if "engine" == "polars" else pd_dt


def _cast_to(
    datatype: str, format: Optional[tuple], engine: Literal["pandas", "polars"]
):
    match datatype.lower():
        case "text" | "url":
            return _to(pl.String, "string", engine)
        case "calendar date":
            if not format:
                # raise ValueError(
                #     "Invalid casting to datetime without format information"
                # )
                return _to(pl.Datetime, "datetime", engine)
            elif format and "view" not in format:
                return _to(pl.Date, "datetime", engine)

            match format["view"]:
                # FIX: Date/Datetime
                case "date" | "date_ymd":
                    return _to(pl.Datetime, "datetime %Y-%m-%d", engine)
                case "date_my":
                    return _to(pl.Datetime, "datetime %Y-%m", engine)
                case "date_y":
                    return _to(pl.Datetime, "datetime %Y", engine)
                case "date_time" | "default_date_time" | "iso_8601_date":
                    return _to(pl.Datetime, "datetime", engine)
                case _:
                    raise ValueError(
                        f"Unexpected value in datetime casting: {datatype}, {format}"
                    )
        case "number":
            if not format:
                # raise ValueError("Invalid casting to number without format information")
                return _to(pl.Float32, "float", engine)
            elif format and "noCommas" not in format:
                return _to(pl.Float32, "integer", engine)

            match format["noCommas"]:
                case "true":
                    return _to(pl.Int32, "integer", engine)
                case "false":
                    return _to(pl.Float32, "float", engine)
        case _:
            return _to(pl.String, "string", engine)


def cast_socrata_types(
    datatypes: list[dict], engine: Literal["pandas", "polars"]
) -> dict:
    return {h["name"]: _cast_to(h["data_type"], h["format"], engine) for h in datatypes}
