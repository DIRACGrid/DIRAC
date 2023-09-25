from copy import deepcopy
import collections.abc
from diraccfg import CFG
import difflib
import pprint
import zlib
from DIRAC import initialize

import typer
from pathlib import Path


def merge_json(d, u):
    """
    Merge together two json
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = merge_json(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def merge_cs_sections_json(root_section, default_setup, known_vos, name=None, merge_in=None):
    """
    Recursively merge the Operation sections
    :param root_section: start of the section we are looking at
    :param name: The name of the section, just used for printing
    :param merge_in: name of the section to merge in (if set, do not merge in the root). Only used for Defaults
    """
    print(f"Working on {name}")
    # Create a Defaults section if it doesn't exist
    if merge_in:
        if merge_in not in root_section:
            root_section[merge_in] = {}
        merge_root = root_section[merge_in]
    else:
        merge_root = root_section
    for section in list(root_section):
        # Can only happen when merge_in is Default
        if section == merge_in:
            continue
        elif section == default_setup:
            print(f"Merging {default_setup} to {merge_in}")
            merge_json(merge_root, root_section[default_setup])
        elif section in known_vos:
            print(f"{section} is a vo.")
            merge_cs_sections_json(root_section[section], default_setup, known_vos, name=section)
        else:
            print(f"{section} is unknown, not touching it (other Setup, custom entry...)")


def merge_cs_sections_cfg(cs_cfg: CFG, default_setup, known_vos):
    """Do the merge of the Operation sections using the CFG object directly
    :param cs_cfg: root of the CS
    """
    operation_section = cs_cfg.getAsCFG("/Operations")
    for section in list(operation_section.getAsDict()):
        # Can only happen when merge_in is Default
        if section == "Defaults":
            continue
        if section == default_setup:
            print(f"Merging {default_setup} to Defaults")

            merged_section = operation_section["Defaults"].mergeWith(operation_section[default_setup])
            operation_section.deleteKey("Defaults")
            operation_section.createNewSection(
                "Defaults",
                comment="Automatic merging to remove Setups",
                contents=merged_section,
            )

            # print(f"Removing {default_setup}")
            # operation_section.deleteKey(default_setup)
        elif section in known_vos:
            vo_section = operation_section[section]
            if default_setup in vo_section:
                merged_section = vo_section.mergeWith(vo_section[default_setup])
                operation_section.deleteKey(section)
                operation_section.createNewSection(
                    section,
                    comment="Automatic merging to remove Setups",
                    contents=merged_section,
                )

            print(f"{section} is a vo.")

        else:
            print(f"{section} is unknown, not touching it (other Setup, custom entry...)")

    cs_cfg.deleteKey("/Operations")
    cs_cfg.createNewSection("/Operations", comment="Merging", contents=operation_section)


def compare_dicts(d1, d2):
    """Produces an HTML output of the diff between 2 dicts"""
    return difflib.HtmlDiff().make_file(
        pprint.pformat(d1).splitlines(),
        pprint.pformat(d2).splitlines(),
    )


def main(diff_file: Path = "/tmp/diff.html", setup: str = "", execute_update: bool = False):
    """
    Remove the Setup sections from the Operations section.

    If --diff-file is specified, dump the html comparison at this location

    If --setup is specified, use it instead of the default one

    If --execute-update is set, it will attempt to actually update the CS
    """
    initialize()
    from DIRAC.ConfigurationSystem.Client.ConfigurationClient import ConfigurationClient
    from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData

    cs_cfg = gConfigurationData.getRemoteCFG()

    original_cs_json = cs_cfg.getAsDict()

    cs_json = cs_cfg.getAsDict()

    default_setup = setup if setup else cs_json["DIRAC"]["DefaultSetup"]
    typer.echo(f"Default setup is {default_setup}")
    known_vos = cs_json["Registry"]["VO"]
    typer.echo(f"Known VOs {known_vos}")
    master_cs = cs_json["DIRAC"]["Configuration"]["MasterServer"]
    typer.echo(f"MasterCS {master_cs}")

    # First do a merge using JSON
    merge_cs_sections_json(cs_json["Operations"], default_setup, known_vos, "Operations", "Defaults")
    first_pass_cs_json = deepcopy(cs_json)
    # Do it twice, because runing over it twice shouldn't change the result
    merge_cs_sections_json(cs_json["Operations"], default_setup, known_vos, "Operations", "Defaults")
    # Make sure the output of the first and second pass are the same
    assert first_pass_cs_json == cs_json

    ##############

    # Redo the exercise with the CFG object

    merge_cs_sections_cfg(cs_cfg, default_setup, known_vos)
    first_pass_cs_cfg = cs_cfg.clone()
    merge_cs_sections_cfg(cs_cfg, default_setup, known_vos)
    assert first_pass_cs_cfg.getAsDict() == cs_cfg.getAsDict()

    ##############

    # Finally, make sure we get the same thing in json and CFG

    assert cs_cfg.getAsDict() == cs_json

    ##############

    # Produces diff output

    # print(compare_dicts(original_cs_json["Operations"], cs_cfg.getAsDict("/Operations")))
    with open(diff_file, "w") as f:
        f.write(compare_dicts(original_cs_json["Operations"], cs_json["Operations"]))

    typer.echo(f"Diff written in {diff_file}")

    if execute_update:
        compressed_data = zlib.compress(str(cs_cfg).encode(), 9)
        update_res = ConfigurationClient(url=master_cs).commitNewData(compressed_data)
        if update_res["OK"]:
            typer.echo("Successfuly updated CS")
        else:
            typer.echo(f"Error updating CS: {update_res['Message']}")


if __name__ == "__main__":
    typer.run(main)
