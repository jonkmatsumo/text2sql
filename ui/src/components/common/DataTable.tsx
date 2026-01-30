import React, { useMemo } from "react";
import { LoadingState } from "./LoadingState";
import { EmptyState } from "./EmptyState";

interface Column<T> {
    key: keyof T | string;
    header: string;
    render?: (item: T) => React.ReactNode;
}

interface DataTableProps<T> {
    data: T[];
    columns: Column<T>[];
    onRowClick?: (item: T) => void;
    isLoading?: boolean;
}

export default function DataTable<T>({
    data,
    columns,
    onRowClick,
    isLoading
}: DataTableProps<T>) {
    if (isLoading) {
        return <LoadingState message="Loading data..." />;
    }

    if (!data || data.length === 0) {
        return <EmptyState title="No items found" />;
    }

    return (
        <div className="table-wrap">
            <table>
                <thead>
                    <tr>
                        {columns.map((col, idx) => (
                            <th key={idx}>{col.header}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {data.map((item, rowIdx) => (
                        <tr
                            key={rowIdx}
                            onClick={() => onRowClick?.(item)}
                            style={onRowClick ? { cursor: "pointer" } : {}}
                        >
                            {columns.map((col, colIdx) => (
                                <td key={colIdx}>
                                    {col.render
                                        ? col.render(item)
                                        : (item as any)[col.key] != null
                                            ? String((item as any)[col.key])
                                            : "—"}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}
