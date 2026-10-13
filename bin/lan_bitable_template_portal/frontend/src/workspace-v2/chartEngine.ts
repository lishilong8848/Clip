import { init, use } from 'echarts/core';
import { BarChart, LineChart, PieChart } from 'echarts/charts';
import { AriaComponent, DataZoomComponent, GridComponent, TooltipComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import { UniversalTransition } from 'echarts/features';
use([BarChart, LineChart, PieChart, GridComponent, TooltipComponent, DataZoomComponent, AriaComponent, SVGRenderer, UniversalTransition]);
export { init };
