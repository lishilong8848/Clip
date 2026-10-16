import { init, use } from 'echarts/core';
import { BarChart, LineChart, PieChart, HeatmapChart } from 'echarts/charts';
import { AriaComponent, CalendarComponent, VisualMapComponent, DataZoomComponent, GridComponent, TooltipComponent } from 'echarts/components';
import { SVGRenderer } from 'echarts/renderers';
import { UniversalTransition } from 'echarts/features';
use([BarChart, LineChart, PieChart, HeatmapChart, CalendarComponent, VisualMapComponent, GridComponent, TooltipComponent, DataZoomComponent, AriaComponent, SVGRenderer, UniversalTransition]);
export { init };
