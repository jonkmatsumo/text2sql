import { CopyButton } from "../artifacts/CopyButton";
import {
  buildCopyBundlePayload,
  type CopyBundleMessageInput,
  toPrettyJson,
} from "../../utils/observability";
import { COPY_SQL_METADATA_LABEL } from "../../constants/operatorUi";

interface CopyBundleButtonProps {
  message: CopyBundleMessageInput;
}

export function CopyBundleButton({ message }: CopyBundleButtonProps) {
  if (!message.sql) return null;
  return <CopyButton text={toPrettyJson(buildCopyBundlePayload(message))} label={COPY_SQL_METADATA_LABEL} />;
}
