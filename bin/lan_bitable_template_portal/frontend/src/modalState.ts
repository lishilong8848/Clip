const owners = new Set<symbol>();
let originalOverflow = "";

export function acquireModal() {
  const owner = Symbol();
  if (!owners.size) {
    originalOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
  }
  owners.add(owner);
  return {
    isTop: () => Array.from(owners)[owners.size - 1] === owner,
    release: () => {
      if (owners.delete(owner) && !owners.size) document.body.style.overflow = originalOverflow;
    },
  };
}
