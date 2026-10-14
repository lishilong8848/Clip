import { parse } from '@vue/compiler-sfc';

// NAlert has no type-specific DOM class. Preserve its declared type in V2 only.
export function markFeedbackAlerts(source) {
  if (!source.includes('NAlert') && !source.includes('n-alert')) return source;
  const { descriptor } = parse(source), edits = [];
  function visit(node) {
    if (node.type === 1 && ['NAlert', 'n-alert'].includes(node.tag)) {
      const declared = node.props.find(p => p.type === 6 && p.name === 'type');
      const binding = node.props.find(p => p.type === 7 && p.name === 'bind' && p.arg?.content === 'type');
      const marked = node.props.some(p => p.name === 'data-v2-feedback-kind' || p.arg?.content === 'data-v2-feedback-kind');
      if (!marked) {
        const expression = binding?.exp?.content || JSON.stringify(declared?.value?.content || 'info');
        const attribute = ' :data-v2-feedback-kind="' + expression.replaceAll('&', '&amp;').replaceAll('"', '&quot;') + '"';
        edits.push({ offset: node.loc.start.offset + node.tag.length + 1, attribute });
      }
    }
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template?.ast || {});
  for (const { offset, attribute } of edits.sort((a, b) => b.offset - a.offset)) source = source.slice(0, offset) + attribute + source.slice(offset);
  return source;
}
