#!/usr/bin/env python
import pandas as pd
import typer
from pathlib import Path
from typer import colors
from typing import Annotated


RED = colors.RED
GREEN = colors.GREEN

app = typer.Typer()


def load_se_definition(se_def_path):
    return pd.read_csv(se_def_path, names=["seName", "basePath"], delimiter=";", index_col="seName")


def load_dfc_dump(dfc_dump_path, version):
    fc_dump = pd.read_csv(dfc_dump_path, names=["seName", "lfn", "fc_cks", "size"], delimiter="|")
    fc_dump["fc_cks"] = fc_dump["fc_cks"].str.lower().str.pad(8, fillchar="0")
    fc_dump["version"] = version
    return fc_dump


def load_se_dump(se_dump_path):
    se_dump = pd.read_csv(se_dump_path, names=["pfn", "se_cks"], delimiter="|", index_col="pfn")
    se_dump["se_cks"] = se_dump["se_cks"].str.lower().str.pad(8, fillchar="0")
    se_dump["version"] = "se_dump"
    assert not se_dump.index.duplicated().any(), f"Duplicated entries in SE dump {se_dump[se_dump.index.duplicated()]}"

    return se_dump


@app.command()
def possibly_lost_data(
    fc_dump_file: Annotated[Path, typer.Option(help="DFC dump AFTER the SE dump")],
    se_def_file: Annotated[Path, typer.Option(help="Definition of the SE path")],
    se_dump_file: Annotated[Path, typer.Option(help="Dump of the SE")],
    lost_file_output: Annotated[Path, typer.Option(help="Output file in which to dump lost")] = "lost.csv",
):
    """
    DANGER: make a partial comparison of an SE dump and an FC dump to find lost data
    Be careful because you can't trust the result:
    * if the FC dump is more recent than the SE dump, you may get files that were added on the SE after the dump
    * if the FC dump is older than the SE dump, the file may have been purposedly removed
    """
    se_dump = load_se_dump(se_dump_file)
    se_def = load_se_definition(se_def_file)

    # Compute the PFN for each LFN in the DFC dump

    fc_dump = load_dfc_dump(fc_dump_file, "fc")
    fc_dump = pd.merge(fc_dump, se_def, on="seName")
    fc_dump["pfn"] = fc_dump["basePath"] + fc_dump["lfn"]
    fc_dump.set_index("pfn", inplace=True)

    # Lost files: in both FC dump but not in the SE

    lostData = fc_dump.index.difference(se_dump.index)
    if len(lostData):
        typer.secho(f"Found {len(lostData)} lost files, dumping them in {lost_file_output}", err=True, fg=RED)
        lastDataDetail = fc_dump[fc_dump.index.isin(lostData)]
        lastDataDetail.to_csv(lost_file_output)
    else:
        typer.secho("No dark data found", fg=GREEN)


@app.command()
def possibly_dark_data(
    fc_dump_file: Annotated[Path, typer.Option(help="DFC dump")],
    se_def_file: Annotated[Path, typer.Option(help="Definition of the SE path")],
    se_dump_file: Annotated[Path, typer.Option(help="Dump of the SE")],
    dark_file_output: Annotated[Path, typer.Option(help="Output file in which to dump dark data")] = "dark.csv",
):
    """
    DANGER: make a partial comparison of an SE dump and an FC dump to find dark data.
    Be careful because you can't trust the result:
    * if the FC dump is more recent than the SE dump, you may get files that were already removed
    * if the FC dump is older than the SE dump, you may find files that were added properly after the dump (DANGER)
    """
    se_dump = load_se_dump(se_dump_file)
    se_def = load_se_definition(se_def_file)

    # Compute the PFN for each LFN in the DFC dump

    fc_dump = load_dfc_dump(fc_dump_file, "fc")
    fc_dump = pd.merge(fc_dump, se_def, on="seName")
    fc_dump["pfn"] = fc_dump["basePath"] + fc_dump["lfn"]
    fc_dump.set_index("pfn", inplace=True)

    # Dark data: in the SE dump but not in any of the FC dump

    typer.echo(f"Computing dark data")
    # Find the dark data
    darkData = se_dump.index.difference(fc_dump.index)

    if len(darkData):
        typer.secho(f"Found {len(darkData)} dark data, dumping them in {dark_file_output}", err=True, fg=RED)
        pd.DataFrame(index=darkData).to_csv(dark_file_output)
    else:
        typer.secho("No dark data found", fg=GREEN)


@app.command()
def compare_checksum(
    fc_dump_file: Annotated[Path, typer.Option(help="DFC dump")],
    se_def_file: Annotated[Path, typer.Option(help="Definition of the SE path")],
    se_dump_file: Annotated[Path, typer.Option(help="Dump of the SE")],
    checksum_output: Annotated[
        Path, typer.Option(help="Output file in which to dump checksum difference")
    ] = "cks_diff.csv",
):
    """
    Compare the checksums of a DFC and an SE dump.
    Careful, sometimes the cks are not padded the same way
    """
    se_dump = load_se_dump(se_dump_file)
    se_def = load_se_definition(se_def_file)

    # Compute the PFN for each LFN in the DFC dump

    fc_dump = load_dfc_dump(fc_dump_file, "fc")
    fc_dump = pd.merge(fc_dump, se_def, on="seName")
    fc_dump["pfn"] = fc_dump["basePath"] + fc_dump["lfn"]
    fc_dump.set_index("pfn", inplace=True)

    typer.echo(f"Computing checksum mismath")
    # Find data in both SE and FC
    in_both = se_dump.index.intersection(fc_dump.index)
    # Make a single DF with both info, and only keep pfn in both
    joined = pd.concat([fc_dump, se_dump], axis=1)
    joined = joined[joined.index.isin(in_both)]

    # Filter on non matching checksum
    non_matching = joined.loc[joined["fc_cks"] != joined["se_cks"]][["seName", "lfn", "fc_cks", "se_cks"]]

    if len(non_matching):
        typer.secho(
            f"Found {len(non_matching)} non matching checksum, dumping them in {checksum_output}", err=True, fg=RED
        )
        non_matching.to_csv(checksum_output, index=False)
    else:
        typer.secho("No checksum mismatch found", fg=GREEN)


@app.command()
def threeway(
    old_fc_dump_file: Annotated[Path, typer.Option(help="DFC dump BEFORE the SE dump")],
    new_fc_dump_file: Annotated[Path, typer.Option(help="DFC dump AFTER the SE dump")],
    se_def_file: Annotated[Path, typer.Option(help="Definition of the SE path")],
    se_dump_file: Annotated[Path, typer.Option(help="Dump of the SE")],
    lost_file_output: Annotated[Path, typer.Option(help="Output file in which to dump lost files")] = "lost.csv",
    dark_file_output: Annotated[Path, typer.Option(help="Output file in which to dump dark data")] = "dark.csv",
):
    """
    Make a full comparison of two FC dump and one SE dump
    """
    se_dump = load_se_dump(se_dump_file)
    se_def = load_se_definition(se_def_file)

    # Compute the PFN for each LFN in the DFC dump
    old_fc_dump = load_dfc_dump(old_fc_dump_file, "old_fc")
    old_fc_dump = pd.merge(old_fc_dump, se_def, on="seName")
    old_fc_dump["pfn"] = old_fc_dump["basePath"] + old_fc_dump["lfn"]
    old_fc_dump.set_index("pfn", inplace=True)

    new_fc_dump = load_dfc_dump(new_fc_dump_file, "new_fc")
    new_fc_dump = pd.merge(new_fc_dump, se_def, on="seName")
    new_fc_dump["pfn"] = new_fc_dump["basePath"] + new_fc_dump["lfn"]
    new_fc_dump.set_index("pfn", inplace=True)

    # Dark data: in the SE dump but not in any of the FC dump

    typer.echo(f"Computing dark data")
    # Find the dark data
    darkData = se_dump.index.difference(old_fc_dump.index.union(new_fc_dump.index))

    if len(darkData):
        typer.secho(f"Found {len(darkData)} dark data, dumping them in {dark_file_output}", err=True, fg=RED)
        pd.DataFrame(index=darkData).to_csv(dark_file_output)
    else:
        typer.secho("No dark data found", fg=GREEN)

    # Lost files: in both FC dump but not in the SE

    lostData = (old_fc_dump.index.intersection(new_fc_dump.index)).difference(se_dump.index)
    if len(lostData):
        typer.secho(f"Found {len(lostData)} lost files, dumping them in {lost_file_output}", err=True, fg=RED)
        lastDataDetail = new_fc_dump[new_fc_dump.index.isin(lostData)]
        lastDataDetail.to_csv(lost_file_output)
    else:
        typer.secho("No dark data found", fg=GREEN)


if __name__ == "__main__":
    app()
