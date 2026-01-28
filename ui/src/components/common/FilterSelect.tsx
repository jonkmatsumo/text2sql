import React from "react";

export interface FilterOption {
  value: string;
  label: string;
}

export interface FilterSelectProps {
  label: string;
  value: string;
  options: FilterOption[];
  onChange: (value: string) => void;
  className?: string;
}

export default function FilterSelect({
  label,
  value,
  options,
  onChange,
  className
}: FilterSelectProps) {
  return (
    <div className={`filter-select ${className || ""}`}>
      <label className="filter-select__label">{label}</label>
      <select
        className="filter-select__dropdown"
        value={value}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}
          </option>
        ))}
      </select>
    </div>
  );
}
