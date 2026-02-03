import React from "react";

interface WaterfallMiniMapProps {
  isActive?: boolean;
}

export const WaterfallMiniMap: React.FC<WaterfallMiniMapProps> = ({ isActive = true }) => {
  if (!isActive) return null;
  return (
    <div className="trace-waterfall__minimap">
      <div className="trace-waterfall__minimap-bar" />
      <div className="trace-waterfall__minimap-note">
        Minimap overview (interactive scroll TODO)
      </div>
    </div>
  );
};
