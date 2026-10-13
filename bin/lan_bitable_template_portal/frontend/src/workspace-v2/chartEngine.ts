import { init, use } from 'echarts/core';
import { BarChart, LineChart, PieChart } from 'echarts/charts';
import { GridComponent, TooltipComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import { UniversalTransition } from 'echarts/features';
use([BarChart, LineChart, PieChart, GridComponent, TooltipComponent, SVGRenderer, UniversalTransition]);
export { init };
