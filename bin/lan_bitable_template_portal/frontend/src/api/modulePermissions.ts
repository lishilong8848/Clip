import { readonly, reactive } from "vue";

/**
 * Reusable, reactive module-permission helper used across the portal.
 *
 * The portal's App.vue synchronises the current user's `auth.user.modules`
 * into this module via `setModulePermissions`. Components such as
 * LighthouseAssistant can call `canUseModule('knowledge')` inside a computed
 * and the result will stay reactive as permissions change.
 */

export type ModuleId = string;

export interface ModulePermissionState {
  modules: ModuleId[];
}

const state = reactive<ModulePermissionState>({ modules: [] });

/** Set (or clear) the authorised module id list for the current user. */
export function setModulePermissions(modules: ModuleId[] = []): void {
  state.modules = Array.isArray(modules) ? [...modules] : [];
}

/**
 * Whether the current user is allowed to use a given module.
 * Returns `true` for an empty module id (nothing to check).
 */
export function canUseModule(moduleId: ModuleId): boolean {
  if (!moduleId) return true;
  return state.modules.includes(moduleId);
}

/** Read-only snapshot of the current module permission state. */
export const modulePermissionsState = readonly(state);