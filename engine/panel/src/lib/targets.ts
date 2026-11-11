// Never infer the local cell from registry order or the selected cell.
export function isLocalCell(cellId: string, clusterId?: string): boolean {
  // The admin registry retains the reserved local alias after forming a cluster.
  return cellId === "local" || Boolean(clusterId && cellId === clusterId);
}

export function canRemoveNode(row: { cellId: string; self: boolean; broker?: unknown },
  isSeed: boolean, clusterId?: string): boolean {
  return isSeed && Boolean(row.broker) && !row.self && isLocalCell(row.cellId, clusterId);
}
