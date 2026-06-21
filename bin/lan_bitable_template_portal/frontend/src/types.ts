export type LooseDict = Record<string, any>;

export type ScopeOption = {
  value: string;
  label: string;
};

export type WorkTypeValue = "maintenance" | "change" | "repair" | "power" | "polling" | "adjust";
export type WorkTypeFilterValue = "" | WorkTypeValue;

export type WorkTypeOption = {
  value: WorkTypeValue;
  label: string;
};

export type WorkTypeFilterOption = {
  value: WorkTypeFilterValue;
  label: string;
};
