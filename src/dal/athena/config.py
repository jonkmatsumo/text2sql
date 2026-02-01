from dataclasses import dataclass

from common.config.env import get_env_str


@dataclass(frozen=True)
class AthenaConfig:
    """Configuration required for Athena query-target access."""

    region: str
    workgroup: str
    output_location: str
    database: str

    @classmethod
    def from_env(cls) -> "AthenaConfig":
        """Load Athena config from environment variables."""
        region = get_env_str("AWS_REGION")
        workgroup = get_env_str("ATHENA_WORKGROUP")
        output_location = get_env_str("ATHENA_OUTPUT_LOCATION")
        database = get_env_str("ATHENA_DATABASE")

        missing = [
            name
            for name, value in {
                "AWS_REGION": region,
                "ATHENA_WORKGROUP": workgroup,
                "ATHENA_OUTPUT_LOCATION": output_location,
                "ATHENA_DATABASE": database,
            }.items()
            if not value
        ]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(
                f"Athena query target missing required config: {missing_list}. "
                "Set AWS_REGION, ATHENA_WORKGROUP, ATHENA_OUTPUT_LOCATION, and ATHENA_DATABASE."
            )

        return cls(
            region=region,
            workgroup=workgroup,
            output_location=output_location,
            database=database,
        )
