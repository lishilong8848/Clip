import {parse} from '@vue/compiler-sfc';

function nodes(source, predicate) {
  const found=[];
  function walk(node){if(node.type===1&&predicate(node))found.push(node);for(const child of node.children||[])walk(child);}
  const template=parse(source).descriptor.template||parse('<template>'+source+'</template>').descriptor.template;
  walk(template.ast);return found;
}
const hasClass=(node,name)=>node.props?.some(prop=>prop.name==='class'&&prop.value?.content.split(/\s+/).includes(name));
function one(source,predicate,label){const found=nodes(source,predicate);if(found.length!==1)throw new Error('Cabinet audit anchor changed: '+label);return found[0];}
const body=node=>node.loc.source.replace(/^<[^>]+>/,'').replace(/<\/[^>]+>$/,'');

// V2-only presentation; reuse the original actions, permission gates and count bindings.
export function adaptCabinetAudit(source,kind){
  let code=source;
  code=code.replace('</script>',`function v2CabinetKnownSum(values: unknown[]): number | string {
  return values.every(value=>typeof value==='number'&&Number.isFinite(value)) ? (values as number[]).reduce((sum,value)=>sum+value,0) : '—';
}
</script>`);
  if(kind==='page'){
    const heading=one(code,node=>node.tag==='header'&&hasClass(node,'heading'),'heading');
    const actions=one(heading.loc.source,node=>hasClass(node,'actions'),'actions');
    const children=actions.children.filter(node=>node.type===1);
    const secondary=children.filter(node=>/loadServerStorage|showExports|openExportSchedule/.test(node.loc.source));
    const primary=children.filter(node=>!secondary.includes(node));
    code=code.replace(actions.loc.source,`<div class="actions v2-cabinet-actions"><template v-if="!scope">${body(actions)}</template><template v-else>${primary.map(node=>node.loc.source).join('\n')}<details class="v2-cabinet-more"><summary>更多工具</summary><div>${secondary.map(node=>node.loc.source).join('\n')}</div></details></template></div>`);
    const summary=one(code,node=>node.tag==='section'&&node.props.some(prop=>prop.name==='if'&&prop.exp?.content==="tab === 'overview'"),'room summary');
    code=code.replace('</script>',`function v2RoomRate(room: Dict): string {
  const powered=v2CabinetKnownSum([room.counts?.formal,room.counts?.test]);
  return typeof powered==='number'&&typeof room.total==='number'&&room.total>0 ? (powered/room.total*100).toFixed(1)+'%' : '—';
}
</script>`);
    const cards=`<div class="v2-room-cards">
      <article v-for="room in overview.rooms" :key="room.id" class="v2-room-card">
        <header><button class="link" @click="showRoomRecords(room.id)">{{ room.carrier ? 'B-' + room.id + '运营商机房' : room.name }}<small>{{ room.id }} 包间</small></button><button class="link" @click="showStateRacks('total',room.id)">总数 {{ room.total }}</button></header>
        <dl class="v2-room-key-counts"><div><dt>已上电</dt><dd><button class="link" @click="showStateRacks('powered',room.id)">{{ room.counts.formal + room.counts.test }}</button></dd></div><div><dt>未上电/已下电</dt><dd><button class="link" @click="showStateRacks('off',room.id)">{{ room.counts.off }}</button></dd></div><div><dt>上电率</dt><dd>{{ v2CabinetRateText({...room.counts,total:room.total}) }}</dd></div></dl>
        <div class="v2-room-links"><button class="link" @click="showStateRacks('unknown',room.id)">待核实 {{ room.counts.unknown }}</button><button v-if="room.carrier && !room.sheet" class="link" @click="tab = 'carrier'; rackRoom = room.id; rackState = ''">运营商机房</button><button v-else class="link" @click="tab = 'layout'; selectRoom(room.id)">{{ room.sheet ? '查看平面图' : '查看目录' }}</button></div>
        <details><summary>电源与机柜分类</summary><div class="v2-room-power"><button v-for="state in ['formal','test']" :key="state" class="link" @click="showStateRacks(state,room.id)">{{ stateLabels[state] }} {{ room.counts[state] }}</button></div><dl v-for="type in ['网络机柜','服务器机柜']" :key="type" class="v2-room-type"><dt>{{ type }}</dt><dd>总数 {{ room.types[type] ?? '—' }} · 已上电 {{ room.type_summary?.[type]?.powered ?? ((room.type_counts?.[type]?.formal || 0) + (room.type_counts?.[type]?.test || 0)) }} · 未上电 {{ room.type_summary?.[type]?.off ?? room.type_counts?.[type]?.off ?? '—' }}</dd></dl></details>
      </article>
      <details class="v2-room-totals"><summary>全部包间合计 · {{ roomTotals.total }} 柜</summary><p>已上电 {{ roomTotals.counts.formal + roomTotals.counts.test }} · 未上电/已下电 {{ roomTotals.counts.off }} · 待核实 {{ roomTotals.counts.unknown }}</p><p>正式电 {{ roomTotals.counts.formal }} · 测试电 {{ roomTotals.counts.test }}</p><p v-for="type in ['网络机柜','服务器机柜']" :key="type">{{ type }}：总数 {{ roomTotals.types[type] }} · 已上电 {{ roomTotals.type_summary[type].powered }} · 未上电 {{ roomTotals.type_summary[type].off }}</p></details>
    </div>`;
    const knownCards=cards.replaceAll('room.counts.formal + room.counts.test','v2CabinetKnownSum([room.counts?.formal,room.counts?.test])')
      .replace('v2CabinetRateText({...room.counts,total:room.total})','v2RoomRate(room)')
      .replaceAll('((room.type_counts?.[type]?.formal || 0) + (room.type_counts?.[type]?.test || 0))','v2CabinetKnownSum([room.type_counts?.[type]?.formal,room.type_counts?.[type]?.test])')
      .replaceAll('{{ room.total }}',"{{ room.total ?? '—' }}")
      .replace(/{{ room\.counts\.(off|unknown) }}/g,"{{ room.counts?.$1 ?? '—' }}")
      .replace('{{ room.counts[state] }}',"{{ room.counts?.[state] ?? '—' }}");
    code=code.replace(summary.loc.source,summary.loc.source.replace('class="table-wrap mobile-card-table"','class="table-wrap mobile-card-table v2-room-summary"').replace('<table>',knownCards+'<table>'));
  }else{
    const scope=one(code,node=>node.tag==='select'&&node.props.some(prop=>prop.name==='model'&&prop.exp?.content==='listScope'),'batch scope');
    // The page-header ScopePicker controls the same route/props; keep one scope input.
    code=code.replace(scope.loc.source,'');
    code=code.replace("listScope = ref(props.scope || '')","listScope = ref(buildingOptions.value.some(item=>item.value===props.scope)?props.scope:'')");
    code=code.replace('</script>',`watch(()=>props.scope,()=>{
  listScope.value=buildingOptions.value.some(item=>item.value===props.scope)?props.scope:'';
  if(!batchId&&mode!=='new')void loadList(1);
});
</script>`);
    const table=one(code,node=>node.tag==='table'&&hasClass(node,'batch-list-table'),'batch list');
    const row=one(table.loc.source,node=>node.tag==='tr'&&node.props.some(prop=>prop.name==='for'),'batch row');
    const cells=row.children.filter(node=>node.tag==='td');
    if(cells.length!==10)throw new Error('Cabinet audit batch columns changed');
    const content=cells.map(body);
    const countValues=["{{ item.stats?.total ?? '—' }}", "{{ item.pending_rows ?? item.pending_label ?? '—' }}", "{{ item.stats?.confirmable ?? '—' }}", "{{ v2CabinetKnownSum(['duplicate','conflict','invalid','failed','rollback_failed','rollback_blocked'].map(key=>item.stats?.[key])) }}"];
    const counts=['总数','待处理','可确认','异常'].map((label,index)=>`<div><dt>${label}</dt><dd>${countValues[index]}</dd></div>`).join('');
    const cards=`<div class="v2-batch-cards"><article v-for="item in list.items || []" :key="item.batch_id" class="v2-batch-card" :class="{'todo-batch':item.is_todo}"><header><strong>{{ item.title || item.batch_id.slice(0,8) }}</strong>${content[3]}</header><p>${content[2]}</p><dl>${counts}</dl><footer>${content[9]}<details><summary>批次信息</summary><div class="batch-name">${content[0]}</div><p>来源：${content[1]}</p><p>创建：${content[8]}</p></details></footer></article></div>`;
    code=code.replace(table.loc.source,cards+table.loc.source);
  }
  return code+'\n<style scoped src="../workspace-v2/cabinetAudit.css"></style>\n';
}
