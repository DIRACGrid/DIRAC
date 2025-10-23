from diracx.core.config.schema import Config as DiracxConfig
from pydantic import ValidationError

from DIRAC import S_ERROR, S_OK


def diracxVerifyConfig(cfgData):
    """Verify CS config using DiracX config validation

    Args:
        cfgData: CS config data

    Returns:
        S_OK | S_ERROR: Value: diracx config validation
    """
    cfg = cfgData.getAsDict()
    try:
        validation = DiracxConfig.model_validate(cfg)
    except ValidationError as exc:
        return S_ERROR(exc)
    return S_OK(validation)
