import type LoadingIndicator from "./components/LoadingIndicator.vue";
import type UiTransition from "./components/UiTransition.vue";

declare module "vue" {
  export interface GlobalComponents {
    LoadingIndicator: typeof LoadingIndicator;
    UiTransition: typeof UiTransition;
  }
}
