import json
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


def merge_cs_sections_operations_json(root_section, default_setup, known_vos, name=None, merge_in=None):
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
            merge_cs_sections_operations_json(root_section[section], default_setup, known_vos, name=section)
        else:
            print(f"{section} is unknown, not touching it (other Setup, custom entry...)")


def merge_cs_sections_operations_cfg(cs_cfg: CFG, default_setup, known_vos):
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


def merge_cs_sections_systems_json(cs_json, default_setup: str):
    """
    Recursively merge the Systems sections
    :param root_section: start of the section we are looking at
    :param name: The name of the section, just used for printing
    :param merge_in: name of the section to merge in (if set, do not merge in the root). Only used for Defaults
    """
    for system in list(cs_json["Systems"]):
        instance = cs_json["DIRAC"]["Setups"][default_setup][system]

        # print(f"Merging {default_setup} to {merge_in}")
        merge_json(cs_json["Systems"][system], cs_json["Systems"][system][instance])


def merge_cs_sections_systems_cfg(cs_cfg: CFG, default_setup: str):
    """Do the merge of the System sections using the CFG object directly
    :param cs_cfg: root of the CS
    """
    all_systems_section = cs_cfg.getAsCFG("/Systems")
    for system in list(all_systems_section.getAsDict()):
        instance = cs_cfg["DIRAC"]["Setups"][default_setup][system]

        merged_section = all_systems_section[system].mergeWith(all_systems_section[system][instance])
        all_systems_section.deleteKey(system)
        all_systems_section.createNewSection(
            system,
            comment="Automatic merging to remove Setups",
            contents=merged_section,
        )

    cs_cfg.deleteKey("/Systems")
    cs_cfg.createNewSection("/Systems", comment="Merging", contents=all_systems_section)


def compare_dicts(d1, d2):
    """Produces an HTML output of the diff between 2 dicts"""
    return difflib.HtmlDiff().make_file(
        pprint.pformat(d1).splitlines(),
        pprint.pformat(d2).splitlines(),
    )


def main(
    diff_folder: Path = "/tmp/",
    master_cs_file: Path = "",
    setup: str = "",
    execute_update: bool = False,
):
    """
    Copy the Setup sections from the Operations section into the Default part.
    Copy the Instance sections of the Systems in the System directly.

    If --diff-folder is specified, dump the html comparison at this location

    If --setup is specified, use it instead of the default one

    If --execute-update is set, it will attempt to actually update the CS. This does not work if master_cs_file is specified
    """

    if master_cs_file.is_file():
        if execute_update:
            raise NotImplementedError("Cannot update the Master CS if starting from a config file !")
        cs_cfg = CFG().loadFromFile(master_cs_file)
    else:
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
    try:
        master_cs = cs_json["DIRAC"]["Configuration"]["MasterServer"]
        typer.echo(f"MasterCS {master_cs}")
    except:
        typer.echo(f"No MasterCS, will not be able to commit")

    ######### Update the Operation section

    # First do a merge using JSON
    merge_cs_sections_operations_json(cs_json["Operations"], default_setup, known_vos, "Operations", "Defaults")
    first_pass_cs_json = deepcopy(cs_json)
    # Do it twice, because runing over it twice shouldn't change the result
    merge_cs_sections_operations_json(cs_json["Operations"], default_setup, known_vos, "Operations", "Defaults")
    # Make sure the output of the first and second pass are the same
    assert first_pass_cs_json == cs_json

    ##############

    # Redo the exercise with the CFG object

    merge_cs_sections_operations_cfg(cs_cfg, default_setup, known_vos)
    first_pass_cs_cfg = cs_cfg.clone()
    merge_cs_sections_operations_cfg(cs_cfg, default_setup, known_vos)
    assert first_pass_cs_cfg.getAsDict() == cs_cfg.getAsDict()

    ##############

    # Finally, make sure we get the same thing in json and CFG

    assert cs_cfg.getAsDict() == cs_json

    ##############

    #########  END Update the Operation section

    ######### Update the System section

    # First do a merge using JSON
    merge_cs_sections_systems_json(cs_json, default_setup)
    first_pass_cs_json = deepcopy(cs_json)
    # Do it twice, because runing over it twice shouldn't change the result
    merge_cs_sections_systems_json(cs_json, default_setup)
    # Make sure the output of the first and second pass are the same
    assert first_pass_cs_json == cs_json

    # Redo the exercise with the CFG object
    merge_cs_sections_systems_cfg(cs_cfg, default_setup)
    first_pass_cs_cfg = cs_cfg.clone()
    merge_cs_sections_systems_cfg(cs_cfg, default_setup)
    assert first_pass_cs_cfg.getAsDict() == cs_cfg.getAsDict()

    ##############

    # Finally, make sure we get the same thing in json and CFG

    assert cs_cfg.getAsDict() == cs_json

    ##############

    ######### END Update the System section

    # Produces diff output

    # print(compare_dicts(original_cs_json["Operations"], cs_cfg.getAsDict("/Operations")))
    with open(diff_folder / "diff_operations.html", "w") as f:
        f.write(compare_dicts(original_cs_json["Operations"], cs_json["Operations"]))

    with open(diff_folder / "diff_systems.html", "w") as f:
        f.write(compare_dicts(original_cs_json["Systems"], cs_json["Systems"]))

    typer.echo(f"Diff written in {diff_folder}")
    cs_cfg.writeToFile(diff_folder / "modifed_cs.cfg")
    with open(diff_folder / "modifed_cs.json", "w") as f:
        json.dump(cs_json, f, indent=3)

    if execute_update:
        if not master_cs:
            typer.echo("No master CS found, not updating", error=True)
        compressed_data = zlib.compress(str(cs_cfg).encode(), 9)
        update_res = ConfigurationClient(url=master_cs).commitNewData(compressed_data)
        if update_res["OK"]:
            typer.echo("Successfuly updated CS")
        else:
            typer.echo(f"Error updating CS: {update_res['Message']}")


if __name__ == "__main__":
    typer.run(main)
