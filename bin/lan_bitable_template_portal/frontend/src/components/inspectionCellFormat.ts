// Display formatting only: editing and persistence always use the original value.
export function inspectionCellText(value:unknown,format='General'):string{
  if(value==null)return '';
  if(format==='@'||format==='General')return String(value);
  const code=format.toLowerCase();
  if(/^(h{1,2}:mm)(:ss)?$/.test(code)){
    let h:number,m:number,s:number;
    if(typeof value==='number'){
      const seconds=Math.round(((value%1)+1)%1*86400)%86400;
      h=Math.floor(seconds/3600);m=Math.floor(seconds/60)%60;s=seconds%60;
    }else{
      const match=/^(\d{1,2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?$/.exec(String(value));
      if(!match||+match[1]>23||+match[2]>59||+(match[3]||0)>59)return String(value);
      [h,m,s]=[+match[1],+match[2],+(match[3]||0)];
    }
    return (code.startsWith('hh')?String(h).padStart(2,'0'):h)+':'+String(m).padStart(2,'0')+(code.endsWith(':ss')?':'+String(s).padStart(2,'0'):'');
  }
  // Excel keeps text as text, including numeric-looking strings.
  if(typeof value==='number'&&/^#?,?##0(?:\.0+)?%?$/.test(format)){
    const decimals=format.match(/\.(0+)/)?.[1].length||0;
    return (value*(format.endsWith('%')?100:1)).toLocaleString('en-US',{minimumFractionDigits:decimals,maximumFractionDigits:decimals,useGrouping:format.includes(',')})+(format.endsWith('%')?'%':'');
  }
  if(typeof value==='number'&&/^0(?:\.0+)?%?$/.test(format)){
    const decimals=format.match(/\.(0+)/)?.[1].length||0;
    return (value*(format.endsWith('%')?100:1)).toFixed(decimals)+(format.endsWith('%')?'%':'');
  }
  return String(value);
}
