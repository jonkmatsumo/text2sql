import { CopyButton } from "../artifacts/CopyButton";
import {
  buildCopyBundlePayload,
  type CopyBundleMessageInput,
  toPrettyJson,
} from "../../utils/observability";

interface CopyBundleButtonProps {
  message: CopyBundleMessageInput;
}

export function CopyBundleButton({ message }: CopyBundleButtonProps) {
  if (!message.sql) return null;
  return <CopyButton text={toPrettyJson(buildCopyBundlePayload(message))} label="Copy SQL + metadata" />;
}
