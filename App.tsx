import React, { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { CabinetSelect } from "./components/CabinetSelect";
import { IconCampaign, IconAudience3, IconMoney } from "./components/Icons.tsx";
import {
  IconCreatives,
  IconLogo,
  IconHistory,
  IconSettings,
  IconMisc,
} from "./components/icons/SidebarIcons";
import { TriggersPage } from "./components/TriggersPage";
import { TextSetsPage } from "./components/TextSetsPage";
import { IconText } from "./components/icons/IconText";


declare global {
  interface Window {
    Telegram?: any;
  }
}

type Theme = "light" | "dark";

type Cabinet = {
  id: string;
  name: string;
  token: string;
};

type TextSet = {
  id: string;
  name: string;
  title: string;
  advertiserInfo?: string;
  logoId?: string;
  shortDescription: string;
  longDescription: string;
  button?: string;
  short_text_swap?: string;
  short_text_symbols?: string;
  long_text_swap?: string;
  long_text_symbols?: string;
};

type CreativeItem = {
  id: string;
  url: string;
  name: string;
  type: "video" | "image";
  uploaded?: boolean;
  vkByCabinet?: Record<string, string>; // cabinet_id -> vk_id
  urls?: Record<string, string>; // cabinet_id -> local url
  thumbUrl?: string;
};

type LogoMeta = { id: string; url: string } | null;

type CreativeSet = {
  id: string;
  name: string;
  items: CreativeItem[];
};

type Audience = {
  id: string;
  name: string;
  created?: string;
  type?: "vk";
};

type PresetCompany = {
  presetName: string;
  companyName: string;
  placements?: any[];
  siteAction?: string;
  sitePixel?: string;
  leadform_id?: string;
  targetAction: string;
  trigger: string;
  time?: string;
  url?: string;
  bannerUrl?: string;
  duplicates?: number;
};

type AudienceContainer = {
  id: string;
  name: string;
  audienceIds: string[];
  audienceNames: string[];
  abstractAudiences: string[];
};

type PresetGroup = {
  id: string;
  groupName: string;
  regions: number[];
  gender: "male,female" | "male" | "female";
  age: string;
  interests: number[];
  audienceIds: string[];
  audienceNames: string[];
  abstractAudiences: string[];
  budget: string;
  utm: string;
  containers?: AudienceContainer[];
  bidStrategy: "min" | "cap";
  maxCpa: string;
  placements?: number[];
};

type PresetAd = {
  id: string;
  adName: string;
  textSetId: string | null;
  isNewTextSet?: boolean;
  newTextSetName: string;
  title?: string;
  shortDescription: string;
  longDescription: string;
  advertiserInfo?: string;
  logoId?: string;
  button?: string;
  buttonText?: string; // Текст на кнопке (для leadads)
  videoIds: string[];
  imageIds: string[];
  creativeSetIds: string[];
  url: string;
};

type Preset = {
  fastPreset?: boolean;
  company: PresetCompany;
  groups: PresetGroup[];
  ads: PresetAd[]; // один к одному по группам, но храним отдельно
};

type HistoryItem = {
  cabinet_id: string;
  date_time: string;
  preset_id: string;
  preset_name: string;
  trigger_time: string;
  status: "success" | "error";
  text_error: string | "null";
  code_error?: string | "null";
  id_company?: number[];
};

// === VK Companies ===
interface VkCompany {
  id: number;
  name: string;
  status: string;
  objective: string;
  created: string;
}

interface VkCompanyStats {
  id: number;
  base?: {
    shows?: number;
    clicks?: number;
    goals?: number;
    spent?: string;
    cpc?: string;
    cpa?: string;
  };
}

// === VK Groups ===
interface VkGroup {
  id: number;
  name: string;
  created: string;
  ad_plan_id: number;
  budget_limit_day: number;
  objective: string;
  status: string;
}

interface VkGroupStats {
  id: number;
  base?: {
    shows?: number;
    clicks?: number;
    goals?: number;
    spent?: string;
    cpc?: string;
    cpa?: string;
  };
}

// === VK Ads (Banners) ===
interface VkAd {
  id: number;
  name: string;
  created: string;
  ad_group_id: number;
  moderation_status: string;
  status: string;
}

interface VkAdStats {
  id: number;
  base?: {
    shows?: number;
    clicks?: number;
    goals?: number;
    spent?: string;
    cpc?: string;
    cpa?: string;
  };
}

interface CompaniesColumnConfig {
  id: string;
  label: string;
  width: number;
  visible: boolean;
  sortable: boolean;
  sortField?: string;
  isStatField?: boolean;
}

const DEFAULT_COMPANIES_COLUMNS: CompaniesColumnConfig[] = [
  { id: "name", label: "Название кампании", width: 250, visible: true, sortable: true, sortField: "name" },
  { id: "status", label: "Статус", width: 130, visible: true, sortable: true, sortField: "status" },
  { id: "id", label: "ID кампании", width: 120, visible: true, sortable: true, sortField: "id" },
  { id: "objective", label: "Цель", width: 120, visible: true, sortable: true, sortField: "objective" },
  { id: "goals", label: "Результат", width: 100, visible: true, sortable: true, sortField: "base.goals", isStatField: true },
  { id: "cpa", label: "Цена за результат, ₽", width: 160, visible: true, sortable: true, sortField: "base.cpa", isStatField: true },
  { id: "spent", label: "Потрачено всего, ₽", width: 160, visible: true, sortable: true, sortField: "base.spent", isStatField: true },
  { id: "revenue", label: "Доход", width: 100, visible: true, sortable: true, sortField: "revenue" },
  { id: "profit", label: "Чистый", width: 100, visible: true, sortable: true, sortField: "profit" },
  { id: "clicks", label: "Клики", width: 80, visible: true, sortable: true, sortField: "base.clicks", isStatField: true },
  { id: "cpc", label: "eCPC, ₽", width: 100, visible: true, sortable: true, sortField: "base.cpc", isStatField: true },
  { id: "shows", label: "Показы", width: 100, visible: true, sortable: true, sortField: "base.shows", isStatField: true },
  { id: "created", label: "Дата создания", width: 130, visible: true, sortable: true, sortField: "created" },
];

interface GroupsColumnConfig {
  id: string;
  label: string;
  width: number;
  visible: boolean;
  sortable: boolean;
  sortField?: string;
  isStatField?: boolean;
}

const DEFAULT_GROUPS_COLUMNS: GroupsColumnConfig[] = [
  { id: "companyName", label: "Название компании", width: 200, visible: true, sortable: false },
  { id: "name", label: "Название группы", width: 250, visible: true, sortable: true, sortField: "name" },
  { id: "groupId", label: "ID группы", width: 100, visible: true, sortable: true, sortField: "id" },
  { id: "status", label: "Статус", width: 130, visible: true, sortable: true, sortField: "status" },
  { id: "budget", label: "Бюджет", width: 120, visible: true, sortable: true, sortField: "budget_limit_day" },
  { id: "goals", label: "Результат", width: 100, visible: true, sortable: true, sortField: "base.goals", isStatField: true },
  { id: "cpa", label: "Цена за рез.", width: 140, visible: true, sortable: true, sortField: "base.cpa", isStatField: true },
  { id: "spent", label: "Потрачено", width: 140, visible: true, sortable: true, sortField: "base.spent", isStatField: true },
  { id: "revenue", label: "Доход", width: 100, visible: true, sortable: true, sortField: "revenue" },
  { id: "profit", label: "Чистый", width: 100, visible: true, sortable: true, sortField: "profit" },
  { id: "clicks", label: "Клики", width: 80, visible: true, sortable: true, sortField: "base.clicks", isStatField: true },
  { id: "shows", label: "Показы", width: 100, visible: true, sortable: true, sortField: "base.shows", isStatField: true },
  { id: "created", label: "Дата создания", width: 130, visible: true, sortable: true, sortField: "created" },
];

interface AdsColumnConfig {
  id: string;
  label: string;
  width: number;
  visible: boolean;
  sortable: boolean;
  sortField?: string;
  isStatField?: boolean;
}

const DEFAULT_ADS_COLUMNS: AdsColumnConfig[] = [
  { id: "companyName", label: "Название компании", width: 180, visible: true, sortable: false },
  { id: "groupName", label: "Название группы", width: 180, visible: true, sortable: false },
  { id: "name", label: "Название объявления", width: 250, visible: true, sortable: true, sortField: "name" },
  { id: "adId", label: "ID объявления", width: 110, visible: true, sortable: true, sortField: "id" },
  { id: "status", label: "Статус", width: 130, visible: true, sortable: true, sortField: "moderation_status" },
  { id: "goals", label: "Результат", width: 100, visible: true, sortable: true, sortField: "base.goals", isStatField: true },
  { id: "cpa", label: "Цена за рез.", width: 140, visible: true, sortable: true, sortField: "base.cpa", isStatField: true },
  { id: "spent", label: "Потрачено", width: 140, visible: true, sortable: true, sortField: "base.spent", isStatField: true },
  { id: "revenue", label: "Доход", width: 100, visible: true, sortable: true, sortField: "revenue" },
  { id: "profit", label: "Чистый", width: 100, visible: true, sortable: true, sortField: "profit" },
  { id: "clicks", label: "Клики", width: 80, visible: true, sortable: true, sortField: "base.clicks", isStatField: true },
  { id: "shows", label: "Показы", width: 100, visible: true, sortable: true, sortField: "base.shows", isStatField: true },
  { id: "created", label: "Дата создания", width: 130, visible: true, sortable: true, sortField: "created" },
];

const API_BASE = "/auto_ads/api";


const AVAILABLE_SUB1 = ["krolik", "insta", "nalichkinrf", "1russ", "karakoz_karas", "utkavalutkarf", "vydavayka"];
const CTA_OPTIONS: {label: string; value: string}[] = [
  { label: "Перейти",             value: "visitSite"   },
  { label: "Написать",            value: "write"       },
  { label: "Подробнее",           value: "learnMore"   },
  { label: "Связаться",           value: "contactUs"   },
  { label: "Отправить сообщение", value: "message"     },
  { label: "Начать чат",          value: "startChat"   },
  { label: "Узнать цену",         value: "getPrice"    },
  { label: "Получить предложение",value: "getoffer"    },
];

const CTA_OPTIONS_LEADADS: {label: string; value: string}[] = [
  { label: "Подать заявку",       value: "apply"       },
  { label: "Получить предложение",value: "getoffer"    },
  { label: "Подробнее",           value: "learnMore"   },
  { label: "Попробовать",         value: "try"         },
  { label: "Узнать",              value: "learn"       },
  { label: "Смотреть",            value: "watch"       },
  { label: "Принять участие",     value: "participate" },
  { label: "Написать",            value: "write"       },
  { label: "Откликнуться",        value: "respond"     },
  { label: "Открыть",             value: "open"        },
  { label: "Создать",             value: "create"      },
  { label: "Связаться",           value: "contactUs"   },
  { label: "Заказать",            value: "order"       },
  { label: "Подписаться",         value: "subscribe"   },
  { label: "Записаться",          value: "enroll"      },
];

type CropFormatId = "600x600" | "1080x1350" | "607x1080";

type ImageFormat = {
  id: CropFormatId;
  label: string;
  width: number;
  height: number;
  aspect: number;
};

const IMAGE_FORMATS: ImageFormat[] = [
  { id: "600x600",   label: "600×600",   width: 600,  height: 600,  aspect: 600 / 600 },
  { id: "1080x1350", label: "1080×1350", width: 1080, height: 1350, aspect: 1080 / 1350 },
  { id: "607x1080",  label: "607×1080",  width: 607,  height: 1080, aspect: 607 / 1080 },
];

function buildUploadUrl(params: {
  userId: string;
  cabinetId: string;
  setId?: string;
  setName?: string;
}) {
  const qs = new URLSearchParams({
    user_id: params.userId,
    cabinet_id: params.cabinetId,
  });
  if (params.setId) qs.append("setId", params.setId);
  if (params.setName) qs.append("setName", params.setName);
  return `${API_BASE}/upload?${qs.toString()}`;
}

// начальный кроп по центру под заданный формат
function calcInitialCropForImage(img: HTMLImageElement, fmt: ImageFormat) {
  const iw = img.naturalWidth || 1;
  const ih = img.naturalHeight || 1;
  const targetAspect = fmt.aspect;
  const imgAspect = iw / ih;

  let cw: number;
  let ch: number;

  if (imgAspect > targetAspect) {
    // картинка шире — ограничиваем по высоте
    ch = ih;
    cw = ih * targetAspect;
  } else {
    // картинка уже — ограничиваем по ширине
    cw = iw;
    ch = iw / targetAspect;
  }

  const cx = (iw - cw) / 2;
  const cy = (ih - ch) / 2;

  return { x: cx, y: cy, width: cw, height: ch };
}

// обрезка через canvas → Blob
async function cropImageToBlob(
  image: HTMLImageElement,
  crop: { x: number; y: number; width: number; height: number },
  outWidth: number,
  outHeight: number
): Promise<Blob> {
  const canvas = document.createElement("canvas");
  canvas.width = outWidth;
  canvas.height = outHeight;

  const ctx = canvas.getContext("2d");
  if (!ctx) throw new Error("No 2D context");

  ctx.drawImage(
    image,
    crop.x,
    crop.y,
    crop.width,
    crop.height,
    0,
    0,
    outWidth,
    outHeight
  );

  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => {
        if (!blob) return reject(new Error("Canvas is empty"));
        resolve(blob);
      },
      "image/jpeg",
      0.95
    );
  });
}

type TabId = "campaigns" | "creatives" | "audiences" | "logo" | "textsets" | "history" | "settings" | "misc";
type CampaignsSubTab = "presets" | "companies";
type CompaniesViewTab = "campaigns" | "groups" | "ads";

type View =
  | { type: "home" }
  | { type: "presetEditor"; presetId?: string }
  | { type: "creativeSetEditor"; setId?: string };

const generateId = () => `id_${Math.random().toString(36).slice(2, 10)}`;

const applyDefaultLogoToDraft = (draft: Preset, logoId?: string | null): Preset => {
  if (!logoId) return draft;
  return {
    ...draft,
    ads: draft.ads.map(a => (a.logoId ? a : { ...a, logoId }))
  };
};

const tsFromCreated = (s?: string) => {
  if (!s) return 0;
  // Поддержка формата "YYYY-MM-DD HH:mm:ss"
  const norm = s.includes(" ") ? s.replace(" ", "T") : s;
  const t = Date.parse(norm);
  return Number.isFinite(t) ? t : 0;
};

const formatPresetCreated = (s?: string): string => {
  if (!s) return "";
  let str = s.trim();
  if (str[10] === " ") str = str.slice(0, 10) + "T" + str.slice(11);
  if (!/[zZ]$/.test(str)) str += "Z"; // приводим к UTC-строке

  const ts = Date.parse(str);
  if (!Number.isFinite(ts)) return "";

  const SHIFT_HOURS = 4; // +4 часа
  const d = new Date(ts + SHIFT_HOURS * 3600_000);

  // Читаем UTC-поля, чтобы избежать двойного учета локального пояса
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  const DD = String(d.getUTCDate()).padStart(2, "0");
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const YY = String(d.getUTCFullYear() % 100).padStart(2, "0");

  return `${hh}:${mm} | ${DD}.${MM}.${YY}`;
};

// История: безопасный парсер timestamp из поля date_time
const parseHistoryTS = (val: unknown): number => {
  if (typeof val !== "string") return 0;
  let s = val.trim().replace(/utc/i, "").trim();
  if (s[10] === " ") s = s.slice(0,10) + "T" + s.slice(11);
  if (!/[zZ]$/.test(s)) s += "Z"; // явно UTC
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : 0;
};

const formatHistoryDateTime = (val: unknown, addHoursShift = 4): string => {
  const ts = parseHistoryTS(val);
  if (!ts) return "—";
  const shifted = ts + addHoursShift * 3600_000;
  const d = new Date(shifted);
  
  const DD = String(d.getUTCDate()).padStart(2, "0");
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const YYYY = d.getUTCFullYear();
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  
  return `${DD}.${MM}.${YYYY} в ${hh}:${mm}`;
};

// Хелпер для получения даты из timestamp (YYYY-MM-DD в UTC+4)
const getHistoryDateStr = (val: unknown, addHoursShift = 4): string => {
  const ts = parseHistoryTS(val);
  if (!ts) return "";
  const shifted = ts + addHoursShift * 3600_000;
  const d = new Date(shifted);
  
  const YYYY = d.getUTCFullYear();
  const MM = String(d.getUTCMonth() + 1).padStart(2, "0");
  const DD = String(d.getUTCDate()).padStart(2, "0");
  
  return `${YYYY}-${MM}-${DD}`;
};

// Хелпер для отображения лейбла даты
const getDateLabel = (dateStr: string): string => {
  const now = new Date();
  const todayStr = now.toISOString().slice(0, 10);
  
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = yesterday.toISOString().slice(0, 10);
  
  if (dateStr === todayStr) return "Сегодня";
  if (dateStr === yesterdayStr) return "Вчера";
  
  // Форматируем как DD.MM.YYYY
  const [y, m, d] = dateStr.split("-");
  return `${d}.${m}.${y}`;
};

// === VK Companies helpers ===
const formatVkStatus = (status: string): { text: string; className: string } => {
  const map: Record<string, { text: string; className: string }> = {
    active: { text: "Активна", className: "status-active" },
    blocked: { text: "Остановлена", className: "status-blocked" },
    deleted: { text: "Удалена", className: "status-deleted" },
    created: { text: "Черновик", className: "status-draft" },
  };
  return map[status] || { text: status, className: "" };
};

const formatVkObjective = (obj: string): string => {
  const map: Record<string, string> = {
    socialengagement: "Сообщение",
    site_conversions: "Сайт",
    leadads: "Лидформа",
    appinstalls: "Установка приложения",
    in_app_conversions: "Конверсия в приложении",
    vk_miniapps: "Мини-приложение",
    reach: "Охват",
    traffic: "Трафик",
  };
  return map[obj] || obj;
};

const formatMoney = (val: string | number | undefined): string => {
  if (val === undefined || val === null || val === "") return "0,00 ₽";
  const num = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(num)) return "0,00 ₽";
  return num.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + " ₽";
};

const formatMoneyInt = (val: string | number | undefined): string => {
  if (val === undefined || val === null || val === "") return "0 ₽";
  const num = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(num)) return "0 ₽";
  return Math.round(num).toLocaleString("ru-RU") + " ₽";
};

const formatNumber = (val: number | undefined): string => {
  if (val === undefined || val === null) return "0";
  return val.toLocaleString("ru-RU");
};

const formatVkCreated = (created: string): string => {
  if (!created) return "—";
  const [datePart] = created.split(" ");
  if (!datePart) return created;
  const [y, m, d] = datePart.split("-");
  return `${d}.${m}.${y}`;
};

  /* Форматирование статуса модерации с учётом status + moderation_status
  const formatModerationStatus = (modStatus: string, entityStatus?: string): { text: string; className: string; sortOrder: number } => {
  // Если entity status = blocked и moderation_status = allowed -> "Остановлена"
  if (entityStatus === "blocked" && modStatus === "allowed") {
    return { text: "Остановлена", className: "status-stopped", sortOrder: 2 };
  }
  
  switch (modStatus) {
    case "allowed":
      return { text: "Активна", className: "status-active", sortOrder: 1 };
    case "blocked":
      return { text: "Отклонено", className: "status-rejected", sortOrder: 4 };
    case "banned":
      return { text: "Заблокировано", className: "status-banned", sortOrder: 5 };
    case "pending":
    case "in_progress":
      return { text: "На модерации", className: "status-pending", sortOrder: 3 };
    default:
      return { text: modStatus || "Неизвестно", className: "status-unknown", sortOrder: 6 };
  }
};
*/

const formatBudget = (val: number | undefined): string => {
  if (val === undefined || val === null) return "—";
  return val.toLocaleString("ru-RU", { minimumFractionDigits: 0, maximumFractionDigits: 0 }) + " ₽/день";
};

// --- Универсальное сравнение videoId с item/его vkByCabinet ---
const videoIdMatchesItem = (adVid: string, item: CreativeItem, cabId: string) => {
  if (!adVid) return false;
  if (String(adVid) === String(item.id)) return true;

  const vk = item.vkByCabinet || {};
  if (vk[cabId] && String(vk[cabId]) === String(adVid)) return true;
  return Object.values(vk).some(v => String(v) === String(adVid));
};

// Найти item по ЛЮБОМУ виду id (item.id или vkByCabinet[*])
const findItemByAnyId = (vid: string, allSets: CreativeSet[], cabId: string) => {
  for (const set of allSets) {
    for (const it of set.items) {
      if (videoIdMatchesItem(vid, it, cabId)) return it;
    }
  }
  return null;
};

// === Сохраняем/восстанавливаем скролл выпадающих списков ===
function usePreserveScroll(
  refOrRefs:
    | React.RefObject<HTMLElement | null>
    | Array<React.RefObject<HTMLElement | null>>,
  deps: React.DependencyList = []
) {
  const refs = Array.isArray(refOrRefs) ? refOrRefs : [refOrRefs];
  const snapshot = React.useRef<number[]>([]);

  // перед обновлением (на deps) сохраняем текущие scrollTop
  React.useLayoutEffect(() => {
    snapshot.current = refs.map(r => r.current?.scrollTop ?? 0);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  // после обновления восстанавливаем scrollTop
  React.useLayoutEffect(() => {
    refs.forEach((r, i) => {
      const el = r.current;
      if (el && typeof snapshot.current[i] === "number") {
        el.scrollTop = snapshot.current[i]!;
      }
    });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  // хэндлер на случай когда нужен onScrollCapture (без логики)
  const onScrollCapture = React.useCallback(() => {}, []);
  return { onScrollCapture };
}
// PIXEL
type PixelSelectProps = {
  pixels: SitePixel[];
  value: string;
  disabled?: boolean;
  placeholder?: string;
  onSelect: (px: SitePixel) => void;
  onAdd: () => void;
  onDelete: (px: SitePixel) => void;
};

type SitePixel = {
  pixel: string;   // то, что отображаем в списке
  domain: string;  // то, что подставляем в company.url
};

const PixelSelect: React.FC<PixelSelectProps> = ({
  pixels, value, disabled,
  placeholder = "Выберите пиксель",
  onSelect, onAdd, onDelete
}) => {
  const [open, setOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const pxLabel = (p: SitePixel) => (p.domain ? `${p.domain} - ${p.pixel}` : p.pixel);
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const portalStyle = usePortalDropdownPosition(wrapRef, open && !disabled, {
    desiredHeight: 260,
    zIndex: 200000,
  });
  const { onScrollCapture } = usePreserveScroll(menuRef, [open, q, pixels.length, value]);

  React.useEffect(() => {
    if (!open) return;

    const handler = (e: PointerEvent) => {
      const t = e.target as Node;
      const wrap = wrapRef.current;
      const menu = menuRef.current;

      if (!wrap) return;
      if (wrap.contains(t)) return;
      if (menu && menu.contains(t)) return;

      setOpen(false);
    };

    document.addEventListener("pointerdown", handler, true); // capture!
    return () => document.removeEventListener("pointerdown", handler, true);
  }, [open]);

  const list = React.useMemo(() => {
    const s = q.trim().toLowerCase();
    const arr = [...pixels];
    arr.sort((a,b) => (a.pixel || "").localeCompare((b.pixel || ""), "ru"));
    if (!s) return arr;
    return arr.filter(it =>
      it.pixel.toLowerCase().includes(s) || it.domain.toLowerCase().includes(s)
    );
  }, [pixels, q]);
  
  const selected = React.useMemo(
    () => pixels.find(p => p.pixel === value) || null,
    [pixels, value]
  );

  return (
    <div ref={wrapRef} className="aud-ms" style={{ position: "relative" }}>
      <div
        className="aud-ms-input"
        onClick={() => !disabled && setOpen(true)}
        style={{ opacity: disabled ? 0.6 : 1, cursor: disabled ? "not-allowed" : "pointer" }}
      >
        <input
          placeholder={placeholder}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => !disabled && setOpen(true)}
          disabled={!!disabled}
        />
        <div className="tags">
          {value
            ? <span className="pill active">{selected ? pxLabel(selected) : value}</span>
            : <span className="hint">Не выбран</span>
          }
        </div>
      </div>

      {open && !disabled && createPortal(
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{
            ...portalStyle,
            padding: 8,
          }}
        >
          <div className="menu-section-title" style={{ display: "flex", justifyContent: "space-between", marginBottom: 8 }}>
            <span>Пиксели</span>
            <span style={{ opacity: 0.6, fontSize: 12 }}>{q ? "Поиск" : `${pixels.length}`}</span>
          </div>

          <div className="menu-list" style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {list.map((it) => {
              const active = it.pixel === value;
              return (
                <div
                  key={`${it.pixel}__${it.domain}`}
                  className={`glass`}
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 10,
                    padding: "8px 10px",
                    borderRadius: 10,
                    cursor: "pointer"
                  }}
                  onClick={() => { onSelect(it); setOpen(false); }}
                >
                  <button
                    type="button"
                    className={`pill ${active ? "active" : ""}`}
                    style={{ textAlign: "left", flex: 1 }}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={(e) => { e.preventDefault(); onSelect(it); setOpen(false); }}
                  >
                    {pxLabel(it)}
                  </button>

                  <button
                    type="button"
                    className="icon-button"
                    title="Удалить пиксель"
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={(e) => {
                      e.stopPropagation();
                      onDelete(it);
                    }}
                  >
                    <IconTrash className="icon" />
                  </button>
                </div>
              );
            })}

            {list.length === 0 && (
              <div className="hint">Ничего не найдено</div>
            )}

            <div style={{ marginTop: 8 }}>
              <button
                type="button"
                className="outline-button"
                style={{ width: "100%" }}
                onMouseDown={(e) => e.preventDefault()}
                onClick={(e) => { e.stopPropagation(); onAdd(); }}
              >
                + Добавить новый пиксель
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};

// LeadFormSelect: single-select с поиском и порталом
const LeadFormSelect: React.FC<{
  leadForms: { id: string; name: string }[];
  value: string; // выбранный id
  disabled?: boolean;
  placeholder?: string;
  onSelect: (lf: { id: string; name: string }) => void;
  onRefresh: () => void;
}> = ({ leadForms, value, disabled, placeholder = "Выберите лидформу", onSelect, onRefresh }) => {
  const [open, setOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const portalStyle = usePortalDropdownPosition(wrapRef, open && !disabled, {
    desiredHeight: 260,
    zIndex: 200000,
  });
  const { onScrollCapture } = usePreserveScroll(menuRef, [open, q, leadForms.length, value]);

  React.useEffect(() => {
    if (!open) return;
    const handler = (e: PointerEvent) => {
      const t = e.target as Node;
      const wrap = wrapRef.current;
      const menu = menuRef.current;
      if (!wrap) return;
      if (wrap.contains(t)) return;
      if (menu && menu.contains(t)) return;
      setOpen(false);
    };
    document.addEventListener("pointerdown", handler, true);
    return () => document.removeEventListener("pointerdown", handler, true);
  }, [open]);

  const selected = leadForms.find(l => l.id === value) || null;

  const list = React.useMemo(() => {
    const s = q.trim().toLowerCase();
    if (!s) return [...leadForms];
    return leadForms.filter(lf => lf.name.toLowerCase().includes(s) || lf.id.toLowerCase().includes(s));
  }, [leadForms, q]);

  return (
    <div ref={wrapRef} className="aud-ms" style={{ position: "relative" }}>
      <div
        className="aud-ms-input"
        onClick={() => !disabled && setOpen(true)}
        style={{ opacity: disabled ? 0.6 : 1, cursor: disabled ? "not-allowed" : "pointer" }}
      >
        <input
          placeholder={placeholder}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onFocus={() => !disabled && setOpen(true)}
          disabled={!!disabled}
        />
        <div className="tags">
          {value ? <span className="pill active">{selected ? selected.name : value}</span> : <span className="hint">Не выбрано</span>}
        </div>
      </div>

      {open && !disabled && createPortal(
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{ ...portalStyle, padding: 8 }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
            <strong>Лидформы</strong>
            <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
              <small style={{ opacity: 0.6 }}>{q ? "Поиск" : `${leadForms.length}`}</small>
              <button type="button" title="Обновить список лидформ" onMouseDown={(e) => e.preventDefault()} onClick={() => { onRefresh(); }}>
                ⟳
              </button>
            </div>
          </div>

          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {list.map(lf => {
              const active = lf.id === value;
              return (
                <div
                  key={lf.id}
                  className="glass"
                  style={{ padding: "8px 10px", borderRadius: 10, cursor: "pointer", display: "flex", justifyContent: "space-between", alignItems: "center" }}
                  onClick={() => { onSelect(lf); setOpen(false); }}
                >
                  <button
                    type="button"
                    className={`pill ${active ? "active" : ""}`}
                    style={{ textAlign: "left", flex: 1 }}
                    onMouseDown={(e) => e.preventDefault()}
                  >
                    {lf.name}
                  </button>
                </div>
              );
            })}

            {list.length === 0 && <div className="hint">Ничего не найдено</div>}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};


// === Компонент выпадающего списка аудиторий ===
type AudiencesMultiSelectProps = {
  apiBase: string;
  userId: string;
  cabinetId: string;                  // нужен для поиска по VK
  vkAudiences: Audience[];            // уже загруженные локальные VK-аудитории
  abstractAudiences: { name: string }[];
  selectedVkIds: string[];            // выбранные VK (по id)
  selectedVkNames: string[];
  selectedAbstractNames: string[];    // выбранные абстрактные (по name)
  onChange: (next: {
    vkIds: string[];
    vkNames: string[];
    abstractNames: string[];
  }) => void;
};

const AudiencesMultiSelect: React.FC<AudiencesMultiSelectProps> = ({
  apiBase, userId, cabinetId,
  vkAudiences, abstractAudiences,
  selectedVkIds, selectedVkNames, selectedAbstractNames,
  onChange
}) => {
  const [open, setOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const [loading, setLoading] = React.useState(false);
  const [remote, setRemote] = React.useState<Audience[]>([]);
  const inputRef = React.useRef<HTMLInputElement | null>(null);
  const wrapRef  = React.useRef<HTMLDivElement | null>(null);
  const reqRef = React.useRef<AbortController | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const portalStyle = usePortalDropdownPosition(wrapRef, open, {
    desiredHeight: 260,
    zIndex: 200000,
  });
  const { onScrollCapture } = usePreserveScroll(menuRef, [
    open, q, loading, remote.length, vkAudiences.length
  ]);

  // Закрывать при потере фокуса
  React.useEffect(() => {
    if (!open) return;

    const handler = (e: PointerEvent) => {
      const t = e.target as Node;
      const wrap = wrapRef.current;
      const menu = menuRef.current;

      if (!wrap) return;
      if (wrap.contains(t)) return;
      if (menu && menu.contains(t)) return;

      setOpen(false);
    };

    document.addEventListener("pointerdown", handler, true);
    return () => document.removeEventListener("pointerdown", handler, true);
  }, [open]);
  //Свернутость
  const [abstractCollapsed, setAbstractCollapsed] = React.useState(true); // по умолчанию свернуто
  // 1.5 сек дебаунс и поиск последних 50 по префиксу
  React.useEffect(() => {
    if (!userId || cabinetId === "all") return;
    if (!open) return; // не ищем, когда меню закрыто

    const t = window.setTimeout(() => {
      // отменяем старый запрос
      reqRef.current?.abort();
      const ac = new AbortController();
      reqRef.current = ac;

      setLoading(true);
      (async () => {
        try {
          const j = await apiJson(
            `${apiBase}/vk/audiences/search?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabinetId)}&q=${encodeURIComponent(q)}`,
            { signal: ac.signal }
          );
          setRemote(Array.isArray(j.audiences) ? j.audiences : []);
        } catch (e: any) {
          if (e?.name !== "AbortError") setRemote([]);
        } finally {
          setLoading(false);
        }
      })();
    }, 500); // 500 мс — отзывчивее

    return () => window.clearTimeout(t);
  }, [q, userId, cabinetId, apiBase, open]);

  const toggleVk = async (id: string, shiftKey: boolean = false) => {
    // При Shift-клике добавляем с минусом (исключение аудитории)
    const idToSave = shiftKey ? `-${id}` : id;
    
    // Проверяем, есть ли уже этот id (с минусом или без)
    const existsPositive = selectedVkIds.includes(id);
    const existsNegative = selectedVkIds.includes(`-${id}`);
    const exists = existsPositive || existsNegative;

    if (!exists) {
      const baseName = resolveName(id);
      const nameToSave = shiftKey ? `-${baseName}` : baseName;

      // по желанию: если аудитория пришла из поиска (remote), допишем её в локальный список на бэкенде
      const existsLocal = vkAudiences.some(a => a.id === id);
      if (!existsLocal) {
        const found = remote.find(a => a.id === id);
        if (found) {
          try {
            const updated = [...vkAudiences, found];
            await fetchSecured(`${apiBase}/audiences/save`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ userId, cabinetId, audiences: updated })
            });
          } catch (e) {
            console.warn("save audience name failed", e);
          }
        }
      }

      onChange({
        vkIds:   [...selectedVkIds, idToSave],
        vkNames: [...selectedVkNames, nameToSave],
        abstractNames: selectedAbstractNames
      });
    } else {
      // Удаляем либо положительный, либо отрицательный id
      const idxPositive = selectedVkIds.indexOf(id);
      const idxNegative = selectedVkIds.indexOf(`-${id}`);
      const idx = idxPositive >= 0 ? idxPositive : idxNegative;
      onChange({
        vkIds:   selectedVkIds.filter((_, i) => i !== idx),
        vkNames: selectedVkNames.filter((_, i) => i !== idx),
        abstractNames: selectedAbstractNames
      });
    }
  };

  const resolveName = (id: string): string => {
    const a = vkAudiences.find(x => x.id === id) || remote.find(x => x.id === id);
    return a?.name ?? "Аудитория";
  };

  const toggleAbstract = (name: string) => {
    const next = selectedAbstractNames.includes(name)
      ? selectedAbstractNames.filter(x => x !== name)
      : [...selectedAbstractNames, name];
    onChange({
    vkIds: selectedVkIds,
    vkNames: selectedVkNames,
    abstractNames: next,
    });
  };
  // При открытии меню — подтягиваем последние 50 из VK

  React.useEffect(() => {
    if (!open) return;                  // только когда меню открыто
    if (!userId || cabinetId === "all") return;

    // отменяем предыдущий запрос
    reqRef.current?.abort();
    const ac = new AbortController();
    reqRef.current = ac;

    setLoading(true);
    (async () => {
      try {
        const j = await apiJson(
          `${apiBase}/vk/audiences/search?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabinetId)}&q=`,
          { signal: ac.signal }
        );
        setRemote(Array.isArray(j.audiences) ? j.audiences : []);
      } catch (e: any) {
        if (e?.name !== "AbortError") setRemote([]);
      } finally {
        setLoading(false);
      }
    })();

    return () => ac.abort();
  }, [open, userId, cabinetId, apiBase]);

  //const tsFromCreated = (s?: string) => (s ? (Date.parse(s) || 0) : 0);
  const sortedLocalVK = [...vkAudiences].sort((a,b) => {
    const dt = tsFromCreated(b.created) - tsFromCreated(a.created);
    if (dt) return dt;
    const ai = parseInt(a.id,10), bi = parseInt(b.id,10);
    if (!Number.isNaN(ai) && !Number.isNaN(bi)) return bi - ai;
    return b.id.localeCompare(a.id);
  });

  // Источник для списка: если есть текст — показываем результат поиска (remote),
  // Разворачиваем результаты поиска, чтобы конец был сверху
  const list = remote.length > 0 ? [...remote].reverse() : sortedLocalVK;

  return (
    <div ref={wrapRef} className="aud-ms" style={{position:"relative"}}>
      <div className="aud-ms-input" onClick={() => setOpen(true)}>
        <input
          ref={inputRef}
          placeholder="Начните вводить название аудитории"
          value={q}
          onChange={e => setQ(e.target.value)}
          onFocus={() => setOpen(true)}
        />
        <div className="tags">
          {/* Показываем выбранные, БЕЗ id */}
          {selectedAbstractNames.map(n => (
            <span key={`ab_${n}`} className="pill active" onClick={(e)=>{e.stopPropagation(); toggleAbstract(n);}}>
              {n} ✕
            </span>
          ))}
          {selectedVkIds.map((id, i) => {
            const isNegative = id.startsWith('-');
            const baseId = isNegative ? id.slice(1) : id;
            const name = selectedVkNames[i] || resolveName(baseId);
            return (
              <span
                key={id}
                className={`pill active ${isNegative ? "negative" : ""}`}
                onClick={(e)=>{ e.stopPropagation(); toggleVk(baseId); }}
              >
                {name} ✕
              </span>
            );
          })}
        </div>
      </div>

      {open && createPortal(
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{
            ...portalStyle,
            padding: 8,
          }}
        >
          {/* Заголовок секции + переключатель */}
          <button
            type="button"
            className="menu-section-title"
            onClick={() => setAbstractCollapsed(v => !v)}
            onMouseDown={(e) => e.preventDefault()}
            style={{
              display:"flex",
              alignItems:"center",
              justifyContent:"space-between",
              width:"100%",
              background:"transparent",
              border:"none",
              padding:"6px 4px",
              cursor:"pointer",
              fontWeight:600
            }}
          >
            <span>Абстрактные</span>
            <span style={{opacity:.8, transform: abstractCollapsed ? "rotate(-90deg)" : "none", transition:"transform .15s"}}>
              ▸
            </span>
          </button>
          
          {/* Контент секции — скрываем/показываем */}
          {!abstractCollapsed && (
            <div className="menu-list" style={{display:"flex", flexDirection:"column", gap:6}}>
              {abstractAudiences.length === 0 && <div className="hint">Пока пусто</div>}
              {abstractAudiences.map(a => {
                const active = selectedAbstractNames.includes(a.name);
                return (
                  <button
                    type="button"
                    onMouseDown={(e) => e.preventDefault()}
                    key={`ab_${a.name}`}
                    className={`pill ${active ? "active": ""}`}
                    onClick={() => toggleAbstract(a.name)}
                    style={{textAlign:"left"}}
                  >
                    {a.name}
                  </button>
                );
              })}
            </div>
          )}

          <div className="menu-section-title" style={{marginTop:10}}>VK</div>
          {q && loading && <div className="hint">Поиск…</div>}
          <div className="menu-list" style={{display:"flex", flexDirection:"column", gap:6}}>
            {list.map(a => {
              const active = selectedVkIds.includes(a.id) || selectedVkIds.includes(`-${a.id}`);
              const isNegative = selectedVkIds.includes(`-${a.id}`);
              return (
                <button
                  type="button"
                  onMouseDown={(e) => e.preventDefault()}
                  key={a.id}
                  className={`pill ${active ? "active": ""} ${isNegative ? "negative" : ""}`}
                  onClick={(e) => toggleVk(a.id, e.shiftKey)}
                  style={{textAlign:"left"}}
                  title={a.created ? `${a.created}${isNegative ? " (исключение)" : ""}` : (isNegative ? "Исключение" : "")}
                >
                  {/* В ИНТЕРФЕЙСЕ id не показываем */}
                  {isNegative ? `−${a.name}` : a.name}
                </button>
              );
            })}
            {list.length === 0 && !loading && <div className="hint">Ничего не найдено</div>}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};

// ===== Дерево интересов с поиском и чипсами =====
type InterestNode = {
  id: number;
  name: string;
  children?: InterestNode[];
};

const InterestsTreeSelect: React.FC<{
  selected: number[];
  onChange: (arr: number[]) => void;
}> = ({ selected, onChange }) => {
  const [tree, setTree] = React.useState<InterestNode[]>([]);
  const [flatById, setFlatById] = React.useState<Record<number, string>>({});
  const [menuOpen, setMenuOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const [expanded, setExpanded] = React.useState<Record<number, boolean>>({});
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const direction = useDropdownDirection(wrapRef as any, menuOpen, 260);
  const inputRef = React.useRef<HTMLInputElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const { onScrollCapture } = usePreserveScroll(menuRef, [
    menuOpen, q, tree.length
  ]);

  React.useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target as Node)) setMenuOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  React.useEffect(() => {
    (async () => {
      try {
        const j = await apiJson(`${API_BASE}/interests`);
        const src: any[] = Array.isArray(j?.interests) ? j.interests : [];
        const norm: InterestNode[] = src.map((cat: any) => ({
          id: Number(cat.id),
          name: String(cat.name || ""),
          children: Array.isArray(cat.children)
            ? cat.children.map((ch: any) => ({
                id: Number(ch.id),
                name: String(ch.name || ""),
              }))
            : [],
        }));
        setTree(norm);

        const flat: Record<number, string> = {};
        (function walk(nodes: InterestNode[]): void {
          for (const n of nodes) {
            flat[n.id] = n.name;
            if (n.children?.length) walk(n.children);
          }
        })(norm);
        setFlatById(flat);
      } catch {}
    })();
  }, []);

  function filtered(nodes: InterestNode[], query: string): InterestNode[] {
    const s = query.trim().toLowerCase();
    if (!s) return nodes;
    function include(arr: InterestNode[]): InterestNode[] {
      const out: InterestNode[] = [];
      for (const n of arr) {
        const hit = n.name.toLowerCase().includes(s);
        const kids = n.children?.length ? include(n.children) : [];
        if (hit || kids.length) out.push({ ...n, children: kids });
      }
      return out;
    }
    return include(nodes);
  }

  function isPlus(id: number): boolean { return selected.includes(Math.abs(id)); }
  function isMinus(id: number): boolean { return selected.includes(-Math.abs(id)); }

  function togglePlus(id: number): void {
    const plusId = Math.abs(id), minusId = -Math.abs(id);
    let next = selected.filter(v => v !== minusId);
    next = next.includes(plusId) ? next.filter(v => v !== plusId) : [...next, plusId];
    onChange(next);
  }

  function toggleMinus(id: number): void {
    const plusId = Math.abs(id), minusId = -Math.abs(id);
    let next = selected.filter(v => v !== plusId);
    next = next.includes(minusId) ? next.filter(v => v !== minusId) : [...next, minusId];
    onChange(next);
  }

  function Row(props: { node: InterestNode; level: number }): React.ReactElement {
    const { node, level } = props;
    const hasChildren = !!node.children?.length;
    const opened = !!expanded[node.id];

    return (
      <div className="tree-card" style={{ margin: 6 }}>
        {/* Шапка карточки родителя/листа */}
        <div className="tree-head">
          {hasChildren ? (
            <button
              className="tree-toggle"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => setExpanded(prev => ({ ...prev, [node.id]: !opened }))}
              title={opened ? "Свернуть" : "Развернуть"}
            >
              <Chevron open={opened} />
            </button>
          ) : (
            <span style={{ width: 18 }} />
          )}

          <span className="tree-title" style={{ paddingLeft: (level ? 16 : 8), opacity: isMinus(node.id) ? 0.6 : 1 }}>
            {isMinus(node.id) ? "— " : ""}
            {node.name}
          </span>

          <div className="tree-actions">
            <button
              className={`pill ${isPlus(node.id) ? "active" : ""}`}
              onClick={() => togglePlus(node.id)}
              onMouseDown={(e) => e.preventDefault()}
              title="Включить (+)"
            >
              +
            </button>
            <button
              className={`pill ${isMinus(node.id) ? "active" : ""}`}
              onClick={() => toggleMinus(node.id)}
              onMouseDown={(e) => e.preventDefault()}
              title="Исключить (–)"
            >
              –
            </button>
          </div>
        </div>

        {/* Дети — внутри той же карточки, с мягкой подложкой */}
        {opened && node.children?.length ? (
          <div className="tree-children">
            {node.children.map(ch => (
              <Row key={ch.id} node={ch} level={level + 1} />
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  function renderTree(nodes: InterestNode[], level = 0): React.ReactNode {
    return nodes.map(n => <Row key={`i_${n.id}`} node={n} level={level} />);
  }

  const list = filtered(tree, q);

  const chips = selected.map(id => {
    const neg = id < 0;
    const pure = Math.abs(id);
    const name = flatById[pure] ?? String(pure);
    return (
      <span
        key={`chip_i_${id}`}
        className="pill active"
        onClick={e => { e.stopPropagation(); onChange(selected.filter(v => v !== id)); }}
      >
        {neg ? "— " : ""}
        {name} ✕
      </span>
    );
  });

  return (
    <div ref={wrapRef} className="aud-ms" style={{ position: "relative" }}>
      <div className="aud-ms-input" onClick={() => { setMenuOpen(true); inputRef.current?.focus(); }}>
        <input
          ref={inputRef}
          placeholder="Начните вводить название интереса"
          value={q}
          onChange={e => setQ(e.target.value)}
          onFocus={() => setMenuOpen(true)}
        />
        <div className="tags">{chips}</div>
      </div>

      {menuOpen && (
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{
            position: "absolute",
            zIndex: 2000,
            left: 0,
            right: 0,
            ...(direction === "up"
              ? { bottom: "100%", marginBottom: 6 }
              : { top: "100%", marginTop: 6 }),
            maxHeight: 260,
            overflow: "auto",
            padding: 8
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
            <div style={{ fontWeight: 600 }}>Интересы</div>
            <div style={{ opacity: 0.7, fontSize: 12 }}>{q ? "Поиск" : "Все категории"}</div>
          </div>
          {list.length ? renderTree(list) : <div className="hint">Ничего не найдено</div>}
        </div>
      )}
    </div>
  );
};
// pads
// ===== Placements (Места размещения) =====
type PlacementNode = {
  key: string;
  label: string;
  id?: number;              // leaf
  children?: PlacementNode[]; // group
};

const PLACEMENTS_BY_TARGET: Record<string, PlacementNode[]> = {
  socialengagement: [
    {
      key: "vk",
      label: "ВКонтакте",
      children: [
        { key: "vk_feed", label: "Лента", id: 1265106 },
        { key: "vk_video", label: "В видео", id: 1010345 },
        { key: "vk_stories", label: "В историях", id: 2243453 },
        { key: "vk_rewarded", label: "Vk mini apps просмотр", id: 1361696 },
        { key: "vk_miniapps_pre", label: "Vk mini apps перед загрузкой", id: 1985149 },
        { key: "vk_miniapps_near", label: "Vk mini apps рядом с контентом", id: 2243456 },
      ],
    },
  ],

  site_conversions: [
    {
      key: "vk",
      label: "ВКонтакте",
      children: [
        { key: "vk_feed", label: "Лента", id: 1302911 },
        { key: "vk_right", label: "Боковая колонка", id: 1302973 },
        { key: "vk_video", label: "В видео", id: 1303013 },
        { key: "vk_miniapps_pre", label: "В VK Mini Apps и играх перед загрузкой или при смене контента", id: 1361696 },
        { key: "vk_clips", label: "В клипах", id: 1303002 },
        { key: "vk_stories", label: "В историях", id: 2243453 },
        { key: "vk_miniapps_near", label: "В VK Mini Apps и играх рядом с контентом", id: 1985149 },
        { key: "vk_rewarded", label: "В VK Mini Apps и играх с вознаграждением за просмотр", id: 2243456 },
        { key: "vk_native", label: "Нативная реклама", id: 2937325 },
      ],
    },
    {
      key: "ok",
      label: "Одноклассники",
      children: [
        { key: "ok_feed", label: "Лента", id: 1302951 },
        { key: "ok_right", label: "Боковая колонка", id: 1302975 },
        { key: "ok_video", label: "В видео", id: 1303027 },
        { key: "ok_miniapps_pre", label: "В Мини-приложениях и играх перед загрузкой или при смене контента", id: 1361695 },
        { key: "ok_near", label: "В приложениях и играх рядом с контентом", id: 2223495 },
        { key: "ok_clips", label: "В клипах", id: 2341238 },
        { key: "ok_rewarded", label: "В Мини-приложениях ОК и играх с вознаграждением за просмотр", id: 2243470 },
      ],
    },
    {
      key: "vk_projects",
      label: "Проекты VK",
      children: [
        { key: "p_native", label: "Нативная реклама", id: 1361614 },
        { key: "p_right", label: "Боковая колонка", id: 1361623 },
        { key: "p_video", label: "В видео", id: 1361628 },
        { key: "p_apps_pre", label: "В приложениях перед загрузкой или при смене контента", id: 1361638 },
        { key: "p_rewarded", label: "В приложениях с вознаграждением за просмотр", id: 2243465 },
      ],
    },
    {
      key: "ad_network",
      label: "Рекламная сеть",
      children: [
        { key: "n_native", label: "Нативная реклама", id: 1359394 },
        { key: "n_right", label: "Боковая колонка", id: 1359123 },
        { key: "n_video", label: "В видео", id: 1359414 },
        { key: "n_apps_pre", label: "В приложениях перед загрузкой или при смене контента", id: 1361691 },
      ],
    },
  ],
};

function collectLeafIds(nodes: PlacementNode[] = []): number[] {
  const out: number[] = [];
  const walk = (n: PlacementNode) => {
    if (typeof n.id === "number") out.push(n.id);
    (n.children || []).forEach(walk);
  };
  nodes.forEach(walk);
  return out;
}

function allowedPlacementIdsForTarget(targetAction: string): Set<number> | null {
  const nodes = PLACEMENTS_BY_TARGET[targetAction];
  if (!nodes) return null;
  return new Set(collectLeafIds(nodes));
}

const IndeterminateCheckbox: React.FC<{
  checked: boolean;
  indeterminate: boolean;
  onChange: () => void;
}> = ({ checked, indeterminate, onChange }) => {
  const ref = React.useRef<HTMLInputElement | null>(null);
  React.useEffect(() => {
    if (ref.current) ref.current.indeterminate = indeterminate;
  }, [indeterminate]);
  return (
    <input
      ref={ref}
      type="checkbox"
      checked={checked}
      onChange={onChange}
      onClick={(e) => e.stopPropagation()}
    />
  );
};

const Chevron: React.FC<{ open: boolean }> = ({ open }) => (
  <svg
    width="16"
    height="16"
    viewBox="0 0 24 24"
    style={{
      transform: open ? "rotate(180deg)" : "rotate(0deg)",
      transition: "transform .15s",
      opacity: 0.85,
    }}
    fill="none"
    stroke="currentColor"
    strokeWidth="2"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <path d="M6 9l6 6 6-6" />
  </svg>
);

const PlacementsTreeSelect: React.FC<{
  targetAction: string;
  selected: number[];
  onChange: (arr: number[]) => void;
}> = ({ targetAction, selected, onChange }) => {
  const nodes = React.useMemo(() => PLACEMENTS_BY_TARGET[targetAction] || [], [targetAction]);

  const [open, setOpen] = React.useState(false);
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const portalStyle = usePortalDropdownPosition(wrapRef, open, {
    desiredHeight: 260,
    zIndex: 200000,
  });
  const { onScrollCapture } = usePreserveScroll(menuRef, [open, selected.join(","), targetAction]);

  const [expanded, setExpanded] = React.useState<Record<string, boolean>>({});

  React.useEffect(() => {
    if (!open) return;
  
    const handler = (e: PointerEvent) => {
      const t = e.target as Node;
      const wrap = wrapRef.current;
      const menu = menuRef.current;
    
      if (!wrap) return;
      if (wrap.contains(t)) return;
      if (menu && menu.contains(t)) return;
    
      setOpen(false);
    };
  
    document.addEventListener("pointerdown", handler, true);
    return () => document.removeEventListener("pointerdown", handler, true);
  }, [open]);
  
  React.useEffect(() => {
    if (open) setExpanded({});
  }, [open, targetAction]);

  const selectedSet = React.useMemo(() => new Set(selected), [selected]);

  const leafIdsOf = React.useCallback((n: PlacementNode): number[] => {
    if (typeof n.id === "number") return [n.id];
    const out: number[] = [];
    (n.children || []).forEach(ch => out.push(...leafIdsOf(ch)));
    return out;
  }, []);

  const isChecked = React.useCallback((n: PlacementNode): boolean => {
    const ids = leafIdsOf(n);
    if (!ids.length) return false;
    return ids.every(id => selectedSet.has(id));
  }, [leafIdsOf, selectedSet]);

  const isIndeterminate = React.useCallback((n: PlacementNode): boolean => {
    const ids = leafIdsOf(n);
    if (!ids.length) return false;
    const any = ids.some(id => selectedSet.has(id));
    const all = ids.every(id => selectedSet.has(id));
    return any && !all;
  }, [leafIdsOf, selectedSet]);

  const toggleNode = (n: PlacementNode) => {
    const ids = leafIdsOf(n);
    if (!ids.length) return;

    const all = ids.every(id => selectedSet.has(id));
    const next = new Set(selected);
    if (all) ids.forEach(id => next.delete(id));
    else ids.forEach(id => next.add(id));

    onChange(Array.from(next));
  };

  // chips: если выбран весь раздел — показываем чип раздела, иначе — чипы листьев
  const chips = React.useMemo(() => {
    const res: Array<{ key: string; label: string; onRemove: () => void }> = [];

    for (const root of nodes) {
      const ids = leafIdsOf(root);
      if (!ids.length) continue;

      const all = ids.every(id => selectedSet.has(id));
      if (all) {
        res.push({
          key: `root_${root.key}`,
          label: root.label,
          onRemove: () => toggleNode(root),
        });
        continue;
      }

      // leaf chips
      const walk = (n: PlacementNode, prefix: string) => {
        if (typeof n.id === "number") {
          if (selectedSet.has(n.id)) {
            res.push({
              key: `leaf_${n.id}`,
              label: `${prefix} · ${n.label}`,
              onRemove: () => toggleNode(n),
            });
          }
          return;
        }
        (n.children || []).forEach(ch => walk(ch, prefix));
      };
      walk(root, root.label);
    }

    return res;
  }, [nodes, leafIdsOf, selectedSet]);

  if (!nodes.length) {
    return (
      <div className="hint" style={{ marginTop: 6 }}>
        Места размещения недоступны для выбранного целевого действия.
      </div>
    );
  }

  return (
    <div ref={wrapRef} className="aud-ms" style={{ position: "relative" }}>
      <div className="aud-ms-input" onClick={() => setOpen(true)}>
        <input
          placeholder="Выберите места размещения"
          value={""}
          readOnly
          onFocus={() => setOpen(true)}
        />
        <div className="tags">
          {chips.map(c => (
            <span
              key={c.key}
              className="pill active"
              onClick={(e) => { e.stopPropagation(); c.onRemove(); }}
            >
              {c.label} ✕
            </span>
          ))}
          {chips.length === 0 && <span className="hint">Не выбрано</span>}
        </div>
      </div>

      {open && createPortal(
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{
            ...portalStyle,
            padding: 8,
          }}
        >
          {nodes.map(root => {
            const opened = expanded[root.key] ?? false;

            return (
              <div key={root.key} className="tree-card" style={{ margin: 6 }}>
                <div className="tree-head" style={{ display: "flex", alignItems: "center", gap: 10 }}>
                  <button
                    className="tree-toggle"
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => setExpanded(prev => ({ ...prev, [root.key]: !opened }))}
                    title={opened ? "Свернуть" : "Развернуть"}
                  >
                    <Chevron open={opened} />
                  </button>

                  <IndeterminateCheckbox
                    checked={isChecked(root)}
                    indeterminate={isIndeterminate(root)}
                    onChange={() => toggleNode(root)}
                  />

                  <div
                    style={{ fontWeight: 600, cursor: "pointer" }}
                    onClick={() => toggleNode(root)}
                  >
                    {root.label}
                  </div>
                </div>

                {opened && (root.children || []).length > 0 && (
                  <div className="tree-children" style={{ marginTop: 8, display: "flex", flexDirection: "column", gap: 1 }}>
                    {(root.children || []).map(ch => {
                      const checked = isChecked(ch);
                      const ind = isIndeterminate(ch);
                      return (
                        <div
                          key={ch.key}
                          className="glass"
                          style={{ padding: "8px 10px", borderRadius: 10, display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}
                          onClick={() => toggleNode(ch)}
                        >
                          <IndeterminateCheckbox
                            checked={checked}
                            indeterminate={ind}
                            onChange={() => toggleNode(ch)}
                          />
                          <div style={{ flex: 1 }}>{ch.label}</div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            );
          })}
        </div>,
        document.body
      )}
    </div>
  );
};

// ===== Дерево регионов с поиском и чипсами =====
type RegionItem = { id: number; name: string; parent_id?: number };
const regionNameCache: Record<number, string> = { "-1": "Весь мир", 188: "Россия" };

const RegionsTreeSelect: React.FC<{
  selected: number[];
  onChange: (arr: number[]) => void;
}> = ({ selected, onChange }) => {
  const [items, setItems] = React.useState<RegionItem[]>([]);
  const [byParent, setByParent] = React.useState<Record<string, RegionItem[]>>({});
  const [nameById, setNameById] = React.useState<Record<number, string>>({ [-1]: "Весь мир", ...regionNameCache });
  const [expanded, setExpanded] = React.useState<Record<number, boolean>>({});
  const [menuOpen, setMenuOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const wrapRef = React.useRef<HTMLDivElement | null>(null);
  const direction = useDropdownDirection(wrapRef as any, menuOpen, 260);
  const inputRef = React.useRef<HTMLInputElement | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const { onScrollCapture } = usePreserveScroll(menuRef, [menuOpen, q, items.length]);

  const ROOT_ID = 188;

  React.useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target as Node)) setMenuOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  React.useEffect(() => {
    (async () => {
      try {
        const j = await apiJson(`${API_BASE}/regions`);
        const arr: RegionItem[] = (Array.isArray(j?.items) ? j.items : []).map((x: any) => ({
          id: Number(x.id),
          name: String(x.name || ""),
          parent_id: x.parent_id != null ? Number(x.parent_id) : undefined,
        }));
        setItems(arr);

        const mp: Record<string, RegionItem[]> = {};
        const nm: Record<number, string> = {};
        for (const it of arr) {
          const key = String(it.parent_id ?? 0);
          (mp[key] ||= []).push(it);
          nm[it.id] = it.name;
        }
        if (!nm[ROOT_ID]) nm[ROOT_ID] = "Россия";
        nm[-1] = "Весь мир"; // Всегда добавляем "Весь мир"
        Object.keys(mp).forEach(k => mp[k].sort((a, b) => a.name.localeCompare(b.name, "ru")));
        setByParent(mp);
        Object.assign(regionNameCache, nm);
        setNameById({ ...regionNameCache });
      } catch {}
    })();
  }, []);

  function childrenOf(pid: number | undefined): RegionItem[] {
    return byParent[String(pid ?? 0)] || [];
  }

  function isPlus(id: number): boolean { return selected.includes(Math.abs(id)); }
  function isMinus(id: number): boolean { return selected.includes(-Math.abs(id)); }

  function applyToggle(id: number, wantMinus: boolean): void {
    const plusId  = Math.abs(id);
    const minusId = -Math.abs(id);
    let next = [...selected];

    // Спец-обработка для "Весь мир" (id === -1 как plusId)
    if (plusId === 1 && id === -1) {
      // Это клик на "Весь мир" — особая логика
      if (wantMinus) {
        // Нельзя исключить "Весь мир" — игнорируем
        return;
      } else {
        // Включаем "Весь мир"
        if (next.includes(-1)) {
          // Уже включён — снимаем
          next = next.filter(v => v !== -1);
          if (next.length === 0) next = [188]; // Если всё сняли — возврат к России
        } else {
          // Включаем "Весь мир": убираем 188 (Россию) и все положительные регионы, оставляем минусы регионов внутри России
          next = next.filter(v => v < 0 && v !== -1); // оставляем только минусы (исключённые регионы внутри России)
          next = [-1, ...next];
        }
        onChange(next);
        return;
      }
    }

    if (wantMinus) {
      // переключаем конкретный минус
      next = next.filter(v => v !== plusId); // убираем возможный плюс того же региона
      next = next.includes(minusId)
        ? next.filter(v => v !== minusId)
        : [...next, minusId];

      // Если выбран "Весь мир", то минусы работают в контексте "Весь мир"
      if (next.includes(-1)) {
        // Убираем 188 и любые положительные (кроме -1)
        next = next.filter(v => v < 0 || v === -1);
        // Убедимся, что -1 есть
        if (!next.includes(-1)) next = [-1, ...next];
      } else {
        // спец-правило: при наличии любых минусов Россия (188) ДОЛЖНА быть включена
        // т.е. разрешаем комбо [188] + (отрицательные регионы)
        next = next.filter(v => v !== -188); // -188 не бывает
        if (next.some(v => v < 0) && !next.includes(188)) {
          next = [188, ...next];
        }

        // не допускаем других плюсов, кроме 188
        next = next.filter(v => v < 0 || v === 188);
      }

    } else {
      // режим включить
      next = next.filter(v => v !== minusId); // убираем возможный минус того же региона
      next = next.includes(plusId)
        ? next.filter(v => v !== plusId)
        : [...next, plusId];

      // Если включаем конкретный регион, убираем "Весь мир" и переходим в стандартный режим
      if (plusId !== 1 && next.includes(-1)) {
        next = next.filter(v => v !== -1);
      }

      // в режиме включения 188 убираем, если выбраны какие-то плюсы
      if (next.some(v => v > 0 && v !== 188 && v !== -1)) {
        next = next.filter(v => Math.abs(v) !== 188);
      }

      // и никаких минусов вместе с плюсами
      next = next.filter(v => v > 0 || v === -1);
    }

    // если вдруг всё сняли — вернуть дефолт 188
    if (next.length === 0) next = [188];

    onChange(next);
  }

  function buildKeepIds(query: string): Set<number> | null {
    const s = query.trim().toLowerCase();
    if (!s) return null;
    const parentOf: Record<number, number | undefined> = {};
    items.forEach(n => { parentOf[n.id] = n.parent_id; });

    const keep = new Set<number>();
    items.forEach(n => {
      if (n.name.toLowerCase().includes(s)) {
        let cur: number | undefined = n.id;
        while (cur !== undefined) {
          keep.add(cur);
          cur = parentOf[cur];
        }
      }
    });
    return keep;
  }

  function Row(
    props: { node: RegionItem; level: number; keep: Set<number> | null }
  ): React.ReactElement | null {
    const { node, level, keep } = props;
    if (keep && !keep.has(node.id)) return null;

    const kids = childrenOf(node.id);
    const opened = !!expanded[node.id];

    return (
      <div className="tree-card" style={{ margin: 6 }}>
        <div className="tree-head">
          {kids.length ? (
            <button
              className="tree-toggle"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => setExpanded(prev => ({ ...prev, [node.id]: !opened }))}
            >
              <Chevron open={opened} />
            </button>
          ) : (
            <span style={{ width: 18 }} />
          )}

          <span className="tree-title" style={{ opacity: isMinus(node.id) ? 0.6 : 1 }}>
            {isMinus(node.id) ? "— " : ""}
            {node.name}
          </span>

          <div className="tree-actions">
            <button
              className={`pill ${isPlus(node.id) ? "active" : ""}`}
              onClick={() => applyToggle(node.id, false)}
              onMouseDown={(e) => e.preventDefault()}
            >
              +
            </button>
            <button
              className={`pill ${isMinus(node.id) ? "active" : ""}`}
              onClick={() => applyToggle(node.id, true)}
              onMouseDown={(e) => e.preventDefault()}
            >
              –
            </button>
          </div>
        </div>

        {opened && kids.length ? (
          <div className="tree-children">
            {kids.map(k => (
              <Row key={`r_${k.id}`} node={k} level={level + 1} keep={keep} />
            ))}
          </div>
        ) : null}
      </div>
    );
  }

  function renderBranchRoot(keep: Set<number> | null): React.ReactNode {
    const rootChildren = childrenOf(ROOT_ID);
    return rootChildren.map(n => (
      <Row key={`r_${n.id}`} node={n} level={0} keep={keep} />
    ));
  }

  const keepIds = buildKeepIds(q);

  const chips = selected.map(id => {
    const neg = id < 0 && id !== -1; // -1 это "Весь мир", не негатив
    const pure = id === -1 ? -1 : Math.abs(id);
    const nm = nameById[pure] ?? String(pure);
    return (
      <span key={`chip_r_${id}`} className="pill active" onClick={e => { e.stopPropagation(); onChange(selected.filter(v => v !== id)); }}>
        {neg ? "— " : ""}
        {nm} ✕
      </span>
    );
  });

  const hasPlusNow = selected.some(v => v > 0);
  const hasMinusNow = selected.some(v => v < 0 && v !== -1); // -1 это "Весь мир", не минус

  return (
    <div ref={wrapRef} className="aud-ms" style={{ position: "relative" }}>
      <div className="aud-ms-input" onClick={() => { setMenuOpen(true); inputRef.current?.focus(); }}>
        <input
          ref={inputRef}
          placeholder="Начните вводить регион"
          value={q}
          onChange={e => setQ(e.target.value)}
          onFocus={() => setMenuOpen(true)}
        />
        <div className="tags">{chips}</div>
      </div>

      {menuOpen && (
        <div
          ref={menuRef}
          className="aud-ms-menu"
          onScrollCapture={onScrollCapture}
          onMouseDown={(e) => e.preventDefault()}
          style={{
            position: "absolute",
            zIndex: 2000,
            left: 0,
            right: 0,
            ...(direction === "up"
              ? { bottom: "100%", marginBottom: 6 }
              : { top: "100%", marginTop: 6 }),
            maxHeight: 260,
            overflow: "auto",
            padding: 8
          }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 6 }}>
            <div style={{ fontWeight: 600 }}>Регионы</div>
            <div className="hint" style={{ fontSize: 12 }}>
              {selected.includes(-1) && "Режим: весь мир"}
              {!selected.includes(-1) && hasPlusNow && "Режим: включить"}
              {!selected.includes(-1) && hasMinusNow && "Режим: исключить"}
              {!selected.includes(-1) && !hasPlusNow && !hasMinusNow && "По умолчанию: 188"}
            </div>
          </div>

          {/* Весь мир - всегда первый */}
          {(!q.trim() || "весь мир".includes(q.trim().toLowerCase())) && (
            <div className="tree-card" style={{ margin: 6 }}>
              <div className="tree-head">
                <span style={{ width: 18 }} />
                <span className="tree-title" style={{ fontWeight: 600 }}>
                  🌍 Весь мир
                </span>
                <div className="tree-actions">
                  <button
                    className={`pill ${selected.includes(-1) ? "active" : ""}`}
                    onClick={() => applyToggle(-1, false)}
                    onMouseDown={(e) => e.preventDefault()}
                  >
                    +
                  </button>
                </div>
              </div>
            </div>
          )}

          {renderBranchRoot(keepIds)}
        </div>
      )}
    </div>
  );
};

async function apiJson(url: string, opts?: RequestInit) {
  const resp = await fetchSecured(url, opts);
  const text = await resp.text();
  let json: any = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`HTTP ${resp.status}: ${text.slice(0, 300)}`);
  }
  if (!resp.ok) {
    // Красивое сообщение на 401
    if (resp.status === 401) {
      throw new Error("Нет доступа: требуется авторизация (Telegram WebApp initData). Откройте приложение из Telegram.");
    }
    const msg = json?.detail || json?.error || `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return json;
}


function buildAuthHeaders(): HeadersInit {
  const tg = (window as any).Telegram?.WebApp;
  // Если работает внутри Telegram WebApp — используем подпись Telegram
  const initData = tg?.initData || "";
  return initData ? { "X-TG-Init-Data": initData } : {};
}

// Обёртка над fetch с автодобавлением заголовков авторизации
async function fetchSecured(input: RequestInfo, init: RequestInit = {}) {
  const mergedHeaders: HeadersInit = {
    ...(init.headers || {}),
    ...buildAuthHeaders(),
  };
  return fetch(input, { ...init, headers: mergedHeaders });
}

async function getQueueStatuses(userId: string, cabinetId: string) {
  // ожидаем ответ вида { items: [{ preset_id, status }] }
  const j = await apiJson(
    `${API_BASE}/queue/status/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabinetId)}`
  );
  const map: Record<string,"active"|"deactive"> = {};
  (j.items || []).forEach((it: any) => {
    if (it?.preset_id) map[String(it.preset_id)] = (it.status === "deactive" ? "deactive" : "active");
  });
  return map;
}

async function setQueueStatusApi(
  userId: string, cabinetId: string, presetId: string, status: "active"|"deactive"
) {
  await fetchSecured(`${API_BASE}/queue/status/set`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ userId, cabinetId, presetId, status })
  });
}

function useDropdownDirection(
  wrapRef: React.RefObject<HTMLElement>,
  isOpen: boolean,
  desiredHeight = 260
) {
  const [dir, setDir] = React.useState<"up" | "down">("down");

  React.useEffect(() => {
    function recalc() {
      const el = wrapRef.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      const spaceBelow = window.innerHeight - r.bottom;
      const spaceAbove = r.top;
      setDir(
        spaceBelow >= desiredHeight || spaceBelow >= spaceAbove ? "down" : "up"
      );
    }
    if (isOpen) {
      recalc();
      window.addEventListener("resize", recalc);
      window.addEventListener("scroll", recalc, true);
      return () => {
        window.removeEventListener("resize", recalc);
        window.removeEventListener("scroll", recalc, true);
      };
    }
  }, [wrapRef, isOpen, desiredHeight]);

  return dir;
}

type PortalPosOpts = {
  desiredHeight?: number;
  offset?: number;     // расстояние от инпута до меню
  minHeight?: number;  // минимальный maxHeight
  zIndex?: number;
};

function usePortalDropdownPosition(
  anchorRef: React.RefObject<HTMLElement | null>,
  isOpen: boolean,
  opts: PortalPosOpts = {}
) {
  const {
    desiredHeight = 260,
    offset = 6,
    minHeight = 140,
    zIndex = 100000,
  } = opts;

  const [style, setStyle] = React.useState<React.CSSProperties>({ display: "none" });

  React.useLayoutEffect(() => {
    if (!isOpen) return;

    const update = () => {
      const el = anchorRef.current;
      if (!el) return;

      const r = el.getBoundingClientRect();

      const spaceBelow = window.innerHeight - r.bottom;
      const spaceAbove = r.top;

      const placeDown = spaceBelow >= desiredHeight || spaceBelow >= spaceAbove;

      const rawMax = (placeDown ? spaceBelow : spaceAbove) - offset - 8;
      const maxHeight = Math.max(minHeight, Math.min(desiredHeight, rawMax));

      // clamp по горизонтали
      const width = r.width;
      const left = Math.max(8, Math.min(r.left, window.innerWidth - width - 8));

      const next: React.CSSProperties = {
        position: "fixed",
        left,
        width,
        zIndex,
        maxHeight,
        overflow: "auto",
        // чтобы меню не влияло на layout
        margin: 0,
      };

      if (placeDown) {
        next.top = Math.min(window.innerHeight - 8, r.bottom + offset);
        next.bottom = "auto";
      } else {
        // «растём вверх» через bottom
        next.bottom = Math.max(8, window.innerHeight - r.top + offset);
        next.top = "auto";
      }

      setStyle(next);
    };

    update();
    window.addEventListener("resize", update);
    // важно: ловим скролл внутри любых контейнеров
    window.addEventListener("scroll", update, true);

    return () => {
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
    };
  }, [anchorRef, isOpen, desiredHeight, offset, minHeight, zIndex]);

  return style;
}

// ===== VK Users Lists (remarketing/users_lists) =====

type UsersListItem = {
  id: number;
  name: string;
  status: string;
  entries_count: number;
  type: string;
  created: string;
};


function formatVkListDate(str: string): string {
  if (!str) return "";
  const [dPart, tPart] = str.split(" ");
  if (!dPart || !tPart) return str;
  const [y, m, d] = dPart.split("-").map(Number);
  const [hh, mm] = tPart.split(":");
  return `${String(d).padStart(2, "0")}.${String(m).padStart(2, "0")}.${y} в ${hh}:${mm}`;
}

type UsersListsTabProps = {
  userId: string;
  cabinetId: string;
};

const UsersListsTab: React.FC<UsersListsTabProps> = ({ userId, cabinetId }) => {
  const [items, setItems] = React.useState<UsersListItem[]>([]);
  const [count, setCount] = React.useState(0);
  const [offset, setOffset] = React.useState<number | null>(null);
  const limit = 200;

  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const [selected, setSelected] = React.useState<Set<number>>(new Set());
  const [action, setAction] = React.useState<"merge" | "per_list" | null>(null);
  const [segmentName, setSegmentName] = React.useState("");
  // --- tabs ---

  const [menuOpen, setMenuOpen] = React.useState(false);
  const kebabRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    if (!menuOpen) return;

    const onDown = (e: PointerEvent) => {
      const t = e.target as Node;
      if (kebabRef.current && kebabRef.current.contains(t)) return;
      setMenuOpen(false);
    };

    document.addEventListener("pointerdown", onDown, true);
    return () => document.removeEventListener("pointerdown", onDown, true);
  }, [menuOpen]);
  // --- SEARCH ---
  const [q, setQ] = React.useState("");
  const [mode, setMode] = React.useState<"startswith" | "contains">("contains");

  const [, setSearchNextOffset] = React.useState<number | null>(null);
  const [searchDone, setSearchDone] = React.useState<boolean>(true);

  const reqRef = React.useRef<AbortController | null>(null);
  const isSearching = q.trim().length > 0;

  // Сбрасываем выбор ТОЛЬКО когда поменялся кабинет или юзер
  React.useEffect(() => {
    setSelected(new Set());
    setAction(null);
    setSegmentName("");
  }, [cabinetId, userId]);

  const loadPage = React.useCallback(
    async (offsetArg: number | null) => {
      setLoading(true);
      setError(null);
      try {
        const qs = new URLSearchParams({
          user_id: String(userId),
          cabinet_id: String(cabinetId),
        });
        if (offsetArg !== null) {
          qs.append("offset", String(offsetArg));
        }

        const data = await apiJson(
          `${API_BASE}/vk/users_lists/page?${qs.toString()}`
        );

        setItems(Array.isArray(data.items) ? data.items : []);
        setCount(Number(data.count || 0));
        setOffset(typeof data.offset === "number" ? data.offset : 0);
      } catch (e: any) {
        setError(e.message || "Ошибка загрузки списков");
      } finally {
        setLoading(false);
      }
    },
    [userId, cabinetId]
  );

  const loadSearch = React.useCallback(
    async (opts: { offset?: number | null; append?: boolean } = {}) => {
      const query = q.trim();

      // отменяем старый запрос
      reqRef.current?.abort();
      const ac = new AbortController();
      reqRef.current = ac;

      setLoading(true);
      setError(null);

      try {
        const qs = new URLSearchParams({
          user_id: String(userId),
          cabinet_id: String(cabinetId),
          q: query,
          mode, // "startswith" | "contains"
          limit: String(limit),          // можно 200 как у тебя
          scan_pages: String(12),        // сколько страниц по 200 просканировать
        });

        // offset: -1 = начать с конца (как на бэке)
        const off = opts.offset == null ? -1 : opts.offset;
        qs.set("offset", String(off));

        const data = await apiJson(`${API_BASE}/vk/users_lists/search?${qs.toString()}`, {
          signal: ac.signal
        });

        const nextItems: UsersListItem[] = Array.isArray(data.items) ? data.items : [];
        const nextCount = Number(data.count || 0);

        // next_offset / done — важны для дозагрузки
        const nextOff =
          typeof data.next_offset === "number" ? data.next_offset : null;
        const done = !!data.done;

        setCount(nextCount);
        setSearchNextOffset(nextOff);
        setSearchDone(done);

        setItems(prev => {
          if (!opts.append) return nextItems;

          // append без дублей по id
          const map = new Map<number, UsersListItem>();
          [...prev, ...nextItems].forEach(it => map.set(it.id, it));
          return Array.from(map.values());
        });

      } catch (e: any) {
        if (e?.name !== "AbortError") {
          setError(e.message || "Ошибка поиска списков");
          if (!opts.append) setItems([]);
        }
      } finally {
        setLoading(false);
      }
    },
    [userId, cabinetId, q, mode, limit]
  );

  React.useEffect(() => {
    if (!userId || !cabinetId) return;
    if (isSearching) return;

    // обычный режим: последняя страница
    loadPage(null);
  }, [userId, cabinetId, isSearching, loadPage]);

  React.useEffect(() => {
    if (!userId || !cabinetId) return;
    if (!isSearching) return;

    const t = window.setTimeout(() => {
      // новый поиск всегда с конца
      loadSearch({ offset: null, append: false });
    }, 400);

    return () => window.clearTimeout(t);
  }, [userId, cabinetId, isSearching, q, mode, loadSearch]);

  const selectedIds = Array.from(selected);
  const canPrev = !isSearching && offset !== null && offset > 0;
  const canNext = !isSearching && offset !== null && count > 0 && offset + limit < count;

  const toggleSelect = (id: number) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const createSegments = async (mode: "merge" | "per_list", baseName?: string) => {
    if (selectedIds.length === 0) return;

    try {
      setLoading(true);
      const res = await fetchSecured(`${API_BASE}/vk/users_lists/create_segments`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId,
          listIds: selectedIds,            // только отмеченные чекбоксами
          mode,
          ...(mode === "merge" ? { baseName: (baseName || "").trim() } : {}), // ТОЛЬКО для merge
        }),
      });

      const data = await res.json();
      if (!res.ok || data.status === "error") {
        throw new Error(data?.error || "Ошибка создания аудиторий");
      }

      // сброс локального состояния
      setAction(null);
      setSegmentName("");
    } catch (e: any) {
      console.error(e);
      alert(e.message || "Ошибка создания аудиторий");
    } finally {
      setLoading(false);
    }
  };

  const handleRunAction = async () => {
    if (action !== "merge") return;
    if (!segmentName.trim() || selectedIds.length === 0) return;
    await createSegments("merge", segmentName.trim());
  };

  const page = !isSearching && offset !== null ? Math.floor(offset / limit) + 1 : 1;
  const totalPages = !isSearching ? Math.max(1, Math.ceil(count / limit)) : 1;
  
  return (
    <div className="users-lists-root">
      <div className="users-lists-toolbar">
        {/* LEFT: счетчики + пагинация */}
        <div className="users-lists-toolbar-left">
          {!isSearching ? (
            <>
              <div className="users-lists-page" title={`Всего списков: ${count}`}>
                {page}/{totalPages} стр
              </div>
          
              <div className="users-lists-pager">
                <button
                  className="nav-arrow"
                  disabled={!canPrev || loading}
                  onClick={() => offset !== null && loadPage(Math.max(0, offset - limit))}
                  type="button"
                  title="Старее"
                >
                  <IconChevronLeft className="icon" />
                </button>
          
                <button
                  className="nav-arrow"
                  disabled={!canNext || loading}
                  onClick={() =>
                    offset !== null &&
                    loadPage(Math.min(Math.max(0, count - limit), offset + limit))
                  }
                  type="button"
                  title="Новее"
                >
                  <IconChevronRight className="icon" />
                </button>
              </div>
            </>
          ) : (
            <div className="users-lists-page">
              Найдено: {items.length}{searchDone ? "" : "…"}
            </div>
          )}
        </div>

        {/* RIGHT: поиск + режим + меню */}
        <div className="users-lists-toolbar-right">
          <select
            value={mode}
            onChange={(e) => setMode(e.target.value as any)}
            className="users-lists-mode"
            disabled={loading}
          >
            <option value="contains">Содержит</option>
            <option value="startswith">Начинается с</option>
          </select>

          <div className="users-lists-search">
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Поиск"
            />
            {!!q.trim() && (
              <button
                className="icon-button"
                title="Очистить"
                onClick={() => {
                  setQ("");
                  setSearchNextOffset(null);
                  setSearchDone(true);
                }}
                disabled={loading}
                type="button"
              >
                ✕
              </button>
            )}
          </div>
        </div>
      </div>

      {error && <div className="error-banner">{error}</div>}

      <div className="users-lists-subbar">
        <div className="users-lists-subbar-left">
          <div className="users-lists-kebab" ref={kebabRef}>
            <button
              className="kebab-button"
              onClick={() => setMenuOpen((v) => !v)}
              title="Действия"
              type="button"
            >
              <IconDots className="kebab-icon" />
            </button>

            {menuOpen && (
              <div className="kebab-menu">
                <button
                  className="kebab-item"
                  disabled={selectedIds.length === 0 || loading}
                  onClick={() => {
                    setMenuOpen(false);
                    setAction("merge");
                  }}
                  type="button"
                >
                  Объединить в аудиторию
                </button>
                
                <button
                  className="kebab-item"
                  disabled={selectedIds.length === 0 || loading}
                  onClick={async () => {
                    setMenuOpen(false);
                    await createSegments("per_list");
                  }}
                  type="button"
                >
                  Создать по аудитории на список
                </button>
              </div>
            )}
          </div>
          
          <div className="hint">Выбрано списков: {selectedIds.length}</div>
        </div>
          
        <button
          className="outline-button"
          disabled={selected.size === 0}
          onClick={() => setSelected(new Set())}
          type="button"
        >
          Снять выделение
        </button>
      </div>

      <div className="users-lists-table">
        <div className="users-lists-header">
          <div />
          <div>Название</div>
          <div>Статус</div>
          <div>Охват</div>
          <div>Тип</div>
          <div>Создан</div>
          <div>ID</div>
        </div>

        {items.map((it) => (
          <div key={it.id} className="users-lists-row">
            <div>
              <input
                type="checkbox"
                checked={selected.has(it.id)}
                onChange={() => toggleSelect(it.id)}
              />
            </div>
            <div title={it.name}>{it.name}</div>
            <div>{it.status}</div>
            <div>{it.entries_count}</div>
            <div>{it.type}</div>
            <div>{formatVkListDate(it.created)}</div>
            <div>{it.id}</div>
          </div>
        ))}

        {items.length === 0 && !loading && (
          <div className="users-lists-empty hint">Списков нет</div>
        )}
      </div>

      {loading && <div className="hint">Загрузка…</div>}

      {action === "merge" && createPortal(
        <div className="popup-overlay" style={{ zIndex: 1000 }}>
          <div className="confirm-window glass">
            <div className="confirm-text">
              Объединить выбранные списки в одну аудиторию
            </div>

            <div className="form-field">
              <label>Название аудитории</label>
              <input
                value={segmentName}
                onChange={(e) => setSegmentName(e.target.value)}
              />
            </div>

            <div className="confirm-actions">
              <button className="outline-button" onClick={() => setAction(null)} type="button">
                Отмена
              </button>
              <button
                className="primary-button"
                disabled={!segmentName.trim() || loading}
                onClick={handleRunAction}
                type="button"
              >
                Создать
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
    </div>
  );
};
// SVG
const IconDots: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" aria-hidden="true" fill="currentColor">
    <circle cx="12" cy="5" r="1.8" />
    <circle cx="12" cy="12" r="1.8" />
    <circle cx="12" cy="19" r="1.8" />
  </svg>
);

const IconChevronLeft: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" aria-hidden="true">
    <path d="M14.5 6.5L9 12l5.5 5.5" />
  </svg>
);

const IconChevronRight: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" aria-hidden="true">
    <path d="M9.5 6.5L15 12l-5.5 5.5" />
  </svg>
);

const IconCopy: React.FC<{ className?: string }> = ({ className }) => (
  <svg
    className={className}
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <rect x="9" y="9" width="10" height="10" rx="2" />
    <rect x="5" y="5" width="10" height="10" rx="2" />
  </svg>
);

const IconTrash: React.FC<{ className?: string }> = ({ className }) => (
  <svg
    className={className}
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <path d="M5 7h14" />
    <path d="M10 11v6" />
    <path d="M14 11v6" />
    <path d="M9 7V5a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2" />
    <path d="M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12" />
  </svg>
);

const IconArrowLeft: React.FC<{ className?: string }> = ({ className }) => (
  <svg
    className={className}
    viewBox="0 0 24 24"
    aria-hidden="true"
  >
    <path
      d="M15 6L9 12L15 18"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

const IconRefresh: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <path d="M1 4v6h6M23 20v-6h-6" />
    <path d="M20.49 9A9 9 0 0 0 5.64 5.64L1 10m22 4l-4.64 4.36A9 9 0 0 1 3.51 15" />
  </svg>
);

const IconDuplicate: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
  </svg>
);

const IconMoreHorizontal: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <circle cx="12" cy="12" r="1.5" />
    <circle cx="6" cy="12" r="1.5" />
    <circle cx="18" cy="12" r="1.5" />
  </svg>
);

const IconPlus: React.FC<{ className?: string }> = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <line x1="12" y1="5" x2="12" y2="19" />
    <line x1="5" y1="12" x2="19" y2="12" />
  </svg>
);

// === Date Range Picker Component ===
const DateRangePicker: React.FC<{
  dateFrom: string;
  dateTo: string;
  isOpen: boolean;
  onChange: (from: string, to: string) => void;
  onToggle: () => void;
  onApply: (from: string, to: string) => void;
}> = ({ dateFrom, dateTo, isOpen, onToggle, onApply, }) => {
  const wrapperRef = React.useRef<HTMLDivElement>(null);
  const [step, setStep] = useState<"from" | "to">("from");
  const [tempFrom, setTempFrom] = useState(dateFrom);
  const [tempTo, setTempTo] = useState(dateTo);
  const [viewMonth, setViewMonth] = useState(() => {
    // Показываем предыдущий месяц как базовый, чтобы справа был текущий
    const now = new Date();
    now.setMonth(now.getMonth() - 1);
    return now;
  });

  const todayStr = new Date().toISOString().slice(0, 10);

  // Reset temps when opening
  React.useEffect(() => {
    if (isOpen) {
      setTempFrom(dateFrom);
      setTempTo(dateTo);
      setStep("from");
      // Устанавливаем viewMonth так, чтобы правый календарь показывал текущий месяц
      const now = new Date();
      now.setMonth(now.getMonth() - 1);
      setViewMonth(now);
    }
  }, [isOpen, dateFrom, dateTo]);

  // Close on click outside
  React.useEffect(() => {
    if (!isOpen) return;
    const handler = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        onToggle();
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [isOpen, onToggle]);

  const formatDisplayDate = (d: string) => {
    if (!d) return "";
    const [y, m, day] = d.split("-");
    const monthNames = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"];
    const monthIdx = parseInt(m, 10) - 1;
    return `${parseInt(day, 10)} ${monthNames[monthIdx]} ${y}`;
  };

  const MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
  ];

  const PRESETS = [
    { label: "Сегодня", getValue: () => { const t = todayStr; return [t, t]; }},
    { label: "Вчера", getValue: () => { const d = new Date(); d.setDate(d.getDate()-1); const t = d.toISOString().slice(0,10); return [t, t]; }},
    { label: "Эта неделя", getValue: () => {
      const now = new Date();
      const day = now.getDay() || 7;
      const start = new Date(now); start.setDate(now.getDate() - day + 1);
      return [start.toISOString().slice(0,10), todayStr];
    }},
    { label: "Последние 7 дней", getValue: () => {
      const start = new Date(); start.setDate(start.getDate() - 6);
      return [start.toISOString().slice(0,10), todayStr];
    }},
    { label: "Этот месяц", getValue: () => {
      const now = new Date();
      const start = new Date(now.getFullYear(), now.getMonth(), 1);
      return [start.toISOString().slice(0,10), todayStr];
    }},
    { label: "Последние 30 дней", getValue: () => {
      const start = new Date(); start.setDate(start.getDate() - 29);
      return [start.toISOString().slice(0,10), todayStr];
    }},
    { label: "Этот год", getValue: () => {
      const now = new Date();
      const start = new Date(now.getFullYear(), 0, 1);
      return [start.toISOString().slice(0,10), todayStr];
    }},
    { label: "Последние 365 дней", getValue: () => {
      const start = new Date();
      start.setDate(start.getDate() - 364);
      return [start.toISOString().slice(0,10), todayStr];
    }},
  ];

  const handleDayClick = (dateStr: string) => {
    // Нельзя выбрать будущие даты
    if (dateStr > todayStr) return;

    if (step === "from") {
      setTempFrom(dateStr);
      setTempTo("");
      setStep("to");
    } else {
      // Ensure from <= to
      if (dateStr < tempFrom) {
        setTempTo(tempFrom);
        setTempFrom(dateStr);
      } else {
        setTempTo(dateStr);
      }
      setStep("from");
    }
  };

  const handlePreset = (from: string, to: string) => {
    setTempFrom(from);
    setTempTo(to);
    setStep("from");
  };

  const handleApply = () => {
    if (tempFrom && tempTo) {
      onApply(tempFrom, tempTo);
      onToggle();
    }
  };

  const renderCalendar = (monthOffset: number) => {
    const baseDate = new Date(viewMonth);
    baseDate.setMonth(baseDate.getMonth() + monthOffset);
    
    const year = baseDate.getFullYear();
    const month = baseDate.getMonth();
    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    
    // Monday start
    let startDow = firstDay.getDay() || 7;
    const startDate = new Date(firstDay);
    startDate.setDate(startDate.getDate() - (startDow - 1));
    
    const weeks: Date[][] = [];
    const cur = new Date(startDate);
    
    for (let w = 0; w < 6; w++) {
      const week: Date[] = [];
      for (let d = 0; d < 7; d++) {
        week.push(new Date(cur));
        cur.setDate(cur.getDate() + 1);
      }
      weeks.push(week);
      if (cur.getMonth() !== month && cur > lastDay) break;
    }

    const monthLabel = `${MONTHS_RU[month].toLowerCase().slice(0,3)} ${year}`;

    return (
      <div className="drp-calendar">
        <div className="drp-month-label">{monthLabel}</div>
        <div className="drp-weekdays">
          {["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"].map(d => (
            <div key={d} className="drp-weekday">{d}</div>
          ))}
        </div>
        <div className="drp-days">
          {weeks.map((week, wi) => (
            <div key={wi} className="drp-week">
              {week.map((day, di) => {
                // Форматируем дату локально, без сдвига таймзоны
                const yyyy = day.getFullYear();
                const mm = String(day.getMonth() + 1).padStart(2, "0");
                const dd = String(day.getDate()).padStart(2, "0");
                const dateStr = `${yyyy}-${mm}-${dd}`;
                
                const isOtherMonth = day.getMonth() !== month;
                const isToday = dateStr === todayStr;
                const isFuture = dateStr > todayStr;
                const isSelected = dateStr === tempFrom || dateStr === tempTo;
                const isInRange = tempFrom && tempTo && dateStr > tempFrom && dateStr < tempTo;
                const isRangeStart = dateStr === tempFrom;
                const isRangeEnd = dateStr === tempTo;
                const isWeekend = di >= 5;

                return (
                  <button
                    key={di}
                    type="button"
                    disabled={isFuture}
                    className={`drp-day ${isOtherMonth ? "other-month" : ""} ${isToday ? "today" : ""} ${isFuture ? "future" : ""} ${isSelected ? "selected" : ""} ${isInRange ? "in-range" : ""} ${isRangeStart ? "range-start" : ""} ${isRangeEnd ? "range-end" : ""} ${isWeekend ? "weekend" : ""}`}
                    onClick={() => handleDayClick(dateStr)}
                  >
                    {day.getDate()}
                  </button>
                );
              })}
            </div>
          ))}
        </div>
      </div>
    );
  };

  const prevMonth = () => setViewMonth(d => { const n = new Date(d); n.setMonth(n.getMonth() - 1); return n; });
  const nextMonth = () => setViewMonth(d => { const n = new Date(d); n.setMonth(n.getMonth() + 1); return n; });

  // Показываем название месяца для ПРАВОГО календаря (viewMonth + 1)
  const rightMonth = new Date(viewMonth);
  rightMonth.setMonth(rightMonth.getMonth() + 1);
  const mainMonthLabel = `${MONTHS_RU[rightMonth.getMonth()]} ${rightMonth.getFullYear()}`;

  return (
    <div className="drp-wrapper" ref={wrapperRef}>
      <button type="button" className="drp-trigger" onClick={onToggle}>
        <span>{formatDisplayDate(dateFrom)}</span>
        <span className="drp-sep">—</span>
        <span>{formatDisplayDate(dateTo)}</span>
      </button>

      {isOpen && (
        <div className="drp-dropdown glass">
          <div className="drp-presets">
            {PRESETS.map(p => (
              <button
                key={p.label}
                type="button"
                className="drp-preset"
                onClick={() => {
                  const [f, t] = p.getValue();
                  handlePreset(f, t);
                }}
              >
                {p.label}
              </button>
            ))}
          </div>

          <div className="drp-main">
            <div className="drp-inputs">
              <input
                type="text"
                value={formatDisplayDate(tempFrom)}
                readOnly
                className={step === "from" ? "active" : ""}
                onClick={() => setStep("from")}
              />
              <span className="drp-sep">—</span>
              <input
                type="text"
                value={formatDisplayDate(tempTo)}
                readOnly
                className={step === "to" ? "active" : ""}
                onClick={() => setStep("to")}
              />
            </div>

            <div className="drp-nav">
              <button type="button" className="drp-nav-btn" onClick={prevMonth}>
                <IconChevronLeft className="icon" />
              </button>
              <span className="drp-nav-label">{mainMonthLabel}</span>
              <button type="button" className="drp-nav-btn" onClick={nextMonth}>
                <IconChevronRight className="icon" />
              </button>
            </div>

            <div className="drp-calendars">
              {renderCalendar(0)}
              {renderCalendar(1)}
            </div>

            <div className="drp-footer">
              <button type="button" className="outline-button" onClick={onToggle}>
                Отменить
              </button>
              <button
                type="button"
                className="primary-button"
                disabled={!tempFrom || !tempTo}
                onClick={handleApply}
              >
                Применить
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Нормализатор пиксель
const normalizeSitePixels = (src: any): SitePixel[] => {
  const arr = Array.isArray(src) ? src : [];
  return arr
    .map((x: any) => {
      if (typeof x === "string") return { pixel: x, domain: "" };

      // поддержка разных форматов от бэка:
      const pixel = String(x?.pixel ?? x?.id ?? x?.name ?? "").trim();
      const domain = String(x?.domain ?? "").trim();

      return { pixel, domain };
    })
    .filter(p => p.pixel.length > 0);
};
// Нормализатор
function normalizePreset(raw: any): Preset {
  const company = {
    presetName: "",
    companyName: "",
    targetAction: "",
    trigger: "time",
    time: "",
    url: "",
    bannerUrl: "",
    siteAction: "uss:success",
    sitePixel: "",
    duplicates: 1,
    ...(raw?.company ?? {})
  };

  const normGroup = (g: any): PresetGroup => ({
    id: g?.id ?? generateId(),
    groupName: g?.groupName ?? "",
    regions: Array.isArray(g?.regions) ? g.regions.map((n:any)=>Number(n)) : [188],
    gender: (g?.gender === "male" || g?.gender === "female" || g?.gender === "male,female") ? g.gender : "male,female",
    age: g?.age ?? "21-55",
    interests: Array.isArray(g?.interests) ? g.interests.map((n:any)=>Number(n)) : [],
    audienceIds: Array.isArray(g?.audienceIds) ? g.audienceIds : [],
    audienceNames: Array.isArray(g?.audienceNames) ? g.audienceNames : [],
    containers: Array.isArray(g?.containers) ? g.containers.map((c:any)=>({
      id: c?.id ?? generateId(),
      name: String(c?.name ?? ""),
      audienceIds: Array.isArray(c?.audienceIds) ? c.audienceIds.map(String) : [],
      audienceNames: Array.isArray(c?.audienceNames) ? c.audienceNames.map(String) : [],
      abstractAudiences: Array.isArray(c?.abstractAudiences) ? c.abstractAudiences.map(String) : [],
    })) : [],
    abstractAudiences: Array.isArray(g?.abstractAudiences) ? g.abstractAudiences : [],
    budget: g?.budget ?? "",
    bidStrategy: (g?.bidStrategy === "cap" || g?.bidStrategy === "min") ? g.bidStrategy : "min",
    maxCpa: g?.maxCpa ?? "",
    placements: Array.isArray(g?.placements) ? g.placements.map((n:any)=>Number(n)) : [],
    utm: g?.utm ?? ""
  });

  const normAd = (a: any): PresetAd => ({
    id: a?.id ?? generateId(),
    adName: a?.adName ?? "",
    textSetId: a?.textSetId ?? null,
    isNewTextSet: !!a?.isNewTextSet,
    newTextSetName: a?.newTextSetName ?? "",
    title: a?.title ?? "",
    shortDescription: a?.shortDescription ?? "",
    longDescription: a?.longDescription ?? "",
    advertiserInfo: a?.advertiserInfo ?? "",
    logoId: a?.logoId ?? "",
    button: a?.button ?? "",
    buttonText: a?.buttonText ?? "",
    videoIds: Array.isArray(a?.videoIds) ? a.videoIds.map(String) : [],
    imageIds: Array.isArray(a?.imageIds) ? a.imageIds.map(String) : [],
    creativeSetIds: Array.isArray(a?.creativeSetIds) ? a.creativeSetIds.map(String) : [],
    url: a?.url ?? ""
  });

  const groups: PresetGroup[] = Array.isArray(raw?.groups) && raw.groups.length
    ? raw.groups.map(normGroup)
    : [normGroup({})];

  const ads: PresetAd[] = Array.isArray(raw?.ads) && raw.ads.length
    ? raw.ads.map(normAd)
    : [normAd({})];

  return { company, groups, ads, fastPreset: !!raw?.fastPreset };
}
const App: React.FC = () => {
  const [userId, setUserId] = useState<string | null>(null);
  const [theme, setTheme] = useState<Theme>("light");
  const [isMobile, setIsMobile] = useState(false);
  const [popup, setPopup] = useState<{open: boolean, msg: string}>({
    open: false,
    msg: ""
  });

  const showPopup = (msg: string) => {
    setPopup({open: true, msg});
    setTimeout(() => setPopup({open: false, msg: ""}), 2000);
  };

  type ViewMode = "grid" | "list";
  type SortBy = "name" | "created" | "trigger";
  type SortDir = "asc" | "desc";

  const [viewMode, setViewMode] = useState<ViewMode>("grid");
  const [sortBy, setSortBy] = useState<SortBy>("created");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const [contextMenu, setContextMenu] = useState<{
    visible: boolean;
    x: number;
    y: number;
    setId: string;
    itemId: string;
  } | null>(null);

  const [queueStatus, setQueueStatus] = useState<Record<string, "active" | "deactive">>({});

  // ----------- AUDIENCE -----------
  const [abstractAudiences, setAbstractAudiences] = useState<{name: string}[]>([]);
  const [newAbstractName, setNewAbstractName] = useState("");
  // ----------- HISTORY -----------
  const [historyDate, setHistoryDate] = useState<string>(() => {
    // По умолчанию — сегодня в формате YYYY-MM-DD
    const now = new Date();
    return now.toISOString().slice(0, 10);
  });

  // === CAMPAIGNS SUBTAB & VK COMPANIES ===
  const [campaignsSubTab, setCampaignsSubTab] = useState<CampaignsSubTab>("presets");
  
  // VK Companies данные
  const [vkCompanies, setVkCompanies] = useState<VkCompany[]>([]);
  const [vkCompaniesStats, setVkCompaniesStats] = useState<Record<number, VkCompanyStats>>({});
  const [vkCompaniesLoading, setVkCompaniesLoading] = useState(false);
  const [vkCompaniesTotal, setVkCompaniesTotal] = useState(0);

  // Сортировка и колонки
  const [companiesSorting, setCompaniesSorting] = useState<{ field: string; dir: "asc" | "desc" }>(() => {
    try {
      const saved = localStorage.getItem("companiesSorting");
      return saved ? JSON.parse(saved) : { field: "created", dir: "desc" };
    } catch {
      return { field: "created", dir: "desc" };
    }
  });

  const [companiesColumns, setCompaniesColumns] = useState<CompaniesColumnConfig[]>(() => {
    try {
      const saved = localStorage.getItem("companiesColumns");
      return saved ? JSON.parse(saved) : DEFAULT_COMPANIES_COLUMNS;
    } catch {
      return DEFAULT_COMPANIES_COLUMNS;
    }
  });

  const [draggedColumnId, setDraggedColumnId] = useState<string | null>(null);

  // Groups sorting and columns
  const [groupsSorting, setGroupsSorting] = useState<{ field: string; dir: "asc" | "desc" }>(() => {
    try {
      const saved = localStorage.getItem("groupsSorting");
      return saved ? JSON.parse(saved) : { field: "created", dir: "desc" };
    } catch {
      return { field: "created", dir: "desc" };
    }
  });

  const [groupsColumns, setGroupsColumns] = useState<GroupsColumnConfig[]>(() => {
    try {
      const saved = localStorage.getItem("groupsColumns");
      return saved ? JSON.parse(saved) : DEFAULT_GROUPS_COLUMNS;
    } catch {
      return DEFAULT_GROUPS_COLUMNS;
    }
  });
  const [draggedGroupColumnId, setDraggedGroupColumnId] = useState<string | null>(null);
  const [selectedGroupIds, setSelectedGroupIds] = useState<Set<number>>(new Set());
  const [groupTogglingIds, setGroupTogglingIds] = useState<Set<number>>(new Set());

  // Ads sorting and columns
  const [adsSorting, setAdsSorting] = useState<{ field: string; dir: "asc" | "desc" }>(() => {
    try {
      const saved = localStorage.getItem("adsSorting");
      return saved ? JSON.parse(saved) : { field: "created", dir: "desc" };
    } catch {
      return { field: "created", dir: "desc" };
    }
  });

  const [adsColumns, setAdsColumns] = useState<AdsColumnConfig[]>(() => {
    try {
      const saved = localStorage.getItem("adsColumns");
      return saved ? JSON.parse(saved) : DEFAULT_ADS_COLUMNS;
    } catch {
      return DEFAULT_ADS_COLUMNS;
    }
  });
  const [draggedAdColumnId, setDraggedAdColumnId] = useState<string | null>(null);
  const [selectedAdIds, setSelectedAdIds] = useState<Set<number>>(new Set());
  const [adTogglingIds, setAdTogglingIds] = useState<Set<number>>(new Set());

  // Companies view tabs and selection
  const [companiesViewTab, setCompaniesViewTab] = useState<CompaniesViewTab>("campaigns");
  const [selectedCompanyIds, setSelectedCompanyIds] = useState<Set<number>>(new Set());
  const [companyTogglingIds, setCompanyTogglingIds] = useState<Set<number>>(new Set()); // для анимации переключения

  // Date picker for companies stats
  // Даты для фильтрации (с сохранением в localStorage)
  const [companiesDateFrom, setCompaniesDateFrom] = useState<string>(() => {
    const saved = localStorage.getItem("companiesDateFrom");
    if (saved) return saved;
    const d = new Date();
    d.setDate(d.getDate() - 6);
    return d.toISOString().slice(0, 10);
  });

  // Закрытие меню колонок при клике вне
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as HTMLElement;
      if (!target.closest('.columns-menu') && !target.closest('.sq-dark-button')) {
        setCompaniesColumnsMenuOpen(false);
        setGroupsColumnsMenuOpen(false);
        setAdsColumnsMenuOpen(false);
      }
    };
    
    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, []);

  const [companiesDateTo, setCompaniesDateTo] = useState<string>(() => {
    const saved = localStorage.getItem("companiesDateTo");
    if (saved) return saved;
    return new Date().toISOString().slice(0, 10);
  });

  // Сохраняем даты в localStorage при изменении
  useEffect(() => {
    localStorage.setItem("companiesDateFrom", companiesDateFrom);
  }, [companiesDateFrom]);

  useEffect(() => {
    localStorage.setItem("companiesDateTo", companiesDateTo);
  }, [companiesDateTo]);

  const [datePickerOpen, setDatePickerOpen] = useState(false);

  const [companiesColumnsMenuOpen, setCompaniesColumnsMenuOpen] = useState(false);
  const [groupsColumnsMenuOpen, setGroupsColumnsMenuOpen] = useState(false);
  const [adsColumnsMenuOpen, setAdsColumnsMenuOpen] = useState(false);

  // Sub1 settings
  const [userSub1, setUserSub1] = useState<string[]>([]);
  const [sub1ModalOpen, setSub1ModalOpen] = useState(false);
  const [sub1Search, setSub1Search] = useState("");
  const [sub1Selected, setSub1Selected] = useState<string[]>([]);

  // VK Groups данные
  const [vkGroups, setVkGroups] = useState<VkGroup[]>([]);
  const [vkGroupsStats, setVkGroupsStats] = useState<Record<number, VkGroupStats>>({});
  const [vkGroupsLoading, setVkGroupsLoading] = useState(false);

  // VK Ads данные
  const [vkAds, setVkAds] = useState<VkAd[]>([]);
  const [vkAdsStats, setVkAdsStats] = useState<Record<number, VkAdStats>>({});
  const [vkAdsLoading, setVkAdsLoading] = useState(false);

  // Revenue data from postback
  const [revenueData, setRevenueData] = useState<Record<string, number>>({});

  const getEntityStatusInfo = (
    entityStatus: string, 
    modStatus: string,
    hasActiveAds?: boolean // новый параметр
  ): { text: string; className: string; sortOrder: number } => {
    // Если entity активна, но нет активных объявлений
    if (entityStatus === "active" && hasActiveAds === false) {
      return { text: "Остановлена", className: "status-stopped", sortOrder: 2 };
    }
    // Если нет активных объявлений (для групп через modStatus)
    if (modStatus === "stopped_no_active_ads") {
      return { text: "Остановлена", className: "status-stopped", sortOrder: 2 };
    }
    // Если entity остановлена пользователем (blocked) но модерация прошла (allowed)
    if (entityStatus === "blocked" && modStatus === "allowed") {
      return { text: "Остановлена", className: "status-stopped", sortOrder: 2 };
    }
    // Если entity активна
    if (entityStatus === "active" && modStatus === "allowed") {
      return { text: "Активна", className: "status-active", sortOrder: 1 };
    }
    // Модерация не пройдена
    if (modStatus === "blocked") {
      return { text: "Отклонена", className: "status-rejected", sortOrder: 4 };
    }
    if (modStatus === "banned") {
      return { text: "Отклонена", className: "status-banned", sortOrder: 5 };
    }
    if (modStatus === "pending" || modStatus === "in_progress") {
      return { text: "На модерации", className: "status-pending", sortOrder: 3 };
    }
    return { text: modStatus || "Неизвестно", className: "status-unknown", sortOrder: 6 };
  };
  
  // Companies totals
  const companiesTotals = useMemo(() => {
    let shows = 0, clicks = 0, goals = 0, spent = 0;
    for (const company of vkCompanies) {
      const stats = vkCompaniesStats[company.id];
      if (stats?.base) {
        shows += stats.base.shows || 0;
        clicks += stats.base.clicks || 0;
        goals += stats.base.goals || 0;
        spent += parseFloat(stats.base.spent || "0") || 0;
      }
    }
    const cpc = clicks > 0 ? spent / clicks : 0;
    const cpa = goals > 0 ? spent / goals : 0;
    return { shows, clicks, goals, spent, cpc, cpa };
  }, [vkCompanies, vkCompaniesStats]);

  // Связи для иерархии
  const [groupsByCompany, setGroupsByCompany] = useState<Record<number, VkGroup[]>>({});
  const [adsByGroup, setAdsByGroup] = useState<Record<number, VkAd[]>>({});

  // Группировка групп по компаниям
  useEffect(() => {
    const map: Record<number, VkGroup[]> = {};
    for (const g of vkGroups) {
      if (!map[g.ad_plan_id]) map[g.ad_plan_id] = [];
      map[g.ad_plan_id].push(g);
    }
    setGroupsByCompany(map);
  }, [vkGroups]);

  // Группировка объявлений по группам
  useEffect(() => {
    const map: Record<number, VkAd[]> = {};
    for (const ad of vkAds) {
      if (!map[ad.ad_group_id]) map[ad.ad_group_id] = [];
      map[ad.ad_group_id].push(ad);
    }
    setAdsByGroup(map);
  }, [vkAds]);

  // Проверка есть ли активные объявления в компании
  const companyHasActiveAds = (companyId: number): boolean => {
    const companyGroups = vkGroups.filter(g => g.ad_plan_id === companyId);
    for (const group of companyGroups) {
      const groupAds = adsByGroup[group.id] || [];
      if (groupAds.some(a => a.status === "active" && a.moderation_status === "allowed")) {
        return true;
      }
    }
    return false;
  };

  // === Revenue & Profit helpers ===
  const getAdRevenue = (adId: number): number => {
    return Math.round(revenueData[String(adId)] || 0);
  };

  const getGroupRevenue = (groupId: number): number => {
    const groupAds = vkAds.filter(ad => ad.ad_group_id === groupId);
    return Math.round(groupAds.reduce((sum, ad) => sum + (revenueData[String(ad.id)] || 0), 0));
  };

  const getCompanyRevenue = (companyId: number): number => {
    const companyGroups = vkGroups.filter(g => g.ad_plan_id === companyId);
    const groupIds = new Set(companyGroups.map(g => g.id));
    const companyAds = vkAds.filter(ad => groupIds.has(ad.ad_group_id));
    return Math.round(companyAds.reduce((sum, ad) => sum + (revenueData[String(ad.id)] || 0), 0));
  };

  const getAdProfit = (adId: number): number => {
    const revenue = getAdRevenue(adId);
    const spent = parseFloat(vkAdsStats[adId]?.base?.spent || "0");
    return Math.round(revenue - spent);
  };

  const getGroupProfit = (groupId: number): number => {
    const revenue = getGroupRevenue(groupId);
    const spent = parseFloat(vkGroupsStats[groupId]?.base?.spent || "0");
    return Math.round(revenue - spent);
  };

  const getCompanyProfit = (companyId: number): number => {
    const revenue = getCompanyRevenue(companyId);
    const spent = parseFloat(vkCompaniesStats[companyId]?.base?.spent || "0");
    return Math.round(revenue - spent);
  };

  // === Sorted VK Companies ===
  const sortedVkCompanies = useMemo(() => {
    const items = [...vkCompanies];
    const { field, dir } = companiesSorting;
    
    items.sort((a, b) => {
      let aVal: any, bVal: any;
      
      if (field.startsWith("base.")) {
        const statField = field.replace("base.", "");
        const aStats = vkCompaniesStats[a.id]?.base || {};
        const bStats = vkCompaniesStats[b.id]?.base || {};
        aVal = parseFloat((aStats as any)[statField] || "0") || 0;
        bVal = parseFloat((bStats as any)[statField] || "0") || 0;
      } else if (field === "created") {
        aVal = a.created || "";
        bVal = b.created || "";
      } else if (field === "status") {
        const aHasActive = companyHasActiveAds(a.id);
        const bHasActive = companyHasActiveAds(b.id);
        
        const getOrder = (status: string, hasActive: boolean) => {
          if (status === "active" && hasActive) return 1;
          if (status === "active" && !hasActive) return 2;
          if (status === "blocked") return 3;
          return 4;
        };
        
        aVal = getOrder(a.status, aHasActive);
        bVal = getOrder(b.status, bHasActive);
      } else if (field === "name") {
        aVal = (a.name || "").toLowerCase();
        bVal = (b.name || "").toLowerCase();
      } else if (field === "id") {
        aVal = a.id;
        bVal = b.id;
      } else if (field === "revenue") {
        aVal = getCompanyRevenue(a.id);
        bVal = getCompanyRevenue(b.id);
      } else if (field === "profit") {
        aVal = getCompanyProfit(a.id);
        bVal = getCompanyProfit(b.id);
      } else {
        aVal = (a as any)[field];
        bVal = (b as any)[field];
      }
      
      let cmp = 0;
      if (typeof aVal === "number" && typeof bVal === "number") {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }
      
      return dir === "asc" ? cmp : -cmp;
    });
    
    return items;
  }, [vkCompanies, vkCompaniesStats, companiesSorting, revenueData, vkGroups, vkAds, adsByGroup]);

  // Вычисление статусов групп на основе объявлений
  const getGroupModerationStatus = (groupId: number): string => {
    const ads = adsByGroup[groupId] || [];
    if (ads.length === 0) return "unknown";
    
    // Проверяем есть ли хоть одно активное объявление
    const hasActiveAd = ads.some(a => a.status === "active" && a.moderation_status === "allowed");
    
    const hasBanned = ads.some(a => a.moderation_status === "banned");
    const hasPending = ads.some(a => a.moderation_status === "pending");
    const hasAllowed = ads.some(a => a.moderation_status === "allowed");
    
    // Если нет активных объявлений - считаем остановленной
    if (!hasActiveAd && hasAllowed) return "stopped_no_active_ads";
    
    if (ads.every(a => a.moderation_status === "banned")) return "banned";
    if (ads.every(a => a.moderation_status === "pending")) return "pending";
    if (ads.every(a => a.moderation_status === "allowed")) return "allowed";
    if (hasBanned && hasAllowed) return "mixed";
    if (hasPending && !hasAllowed) return "pending";
    return "allowed";
  };

  /* Вычисление статусов компаний на основе групп
  const getCompanyModerationStatus = (companyId: number): string => {
    const groups = groupsByCompany[companyId] || [];
    if (groups.length === 0) return "unknown";
    
    const statuses = groups.map(g => getGroupModerationStatus(g.id));
    const hasBanned = statuses.includes("banned");
    const hasMixed = statuses.includes("mixed");
    const hasAllowed = statuses.includes("allowed");
    
    if (statuses.every(s => s === "banned")) return "banned";
    if (statuses.every(s => s === "pending")) return "pending";
    if (hasBanned && hasAllowed) return "mixed";
    if (hasMixed) return "mixed";
    return "allowed";
  };
  */

  // Счётчики выбранных для табов
  const selectedCounts = useMemo(() => {
    const companyCount = selectedCompanyIds.size;
    
    // Получаем группы выбранных компаний
    let groupIds = new Set<number>();
    selectedCompanyIds.forEach(cId => {
      const groups = groupsByCompany[cId] || [];
      groups.forEach(g => groupIds.add(g.id));
    });
    
    // Получаем объявления выбранных групп
    let adIds = new Set<number>();
    groupIds.forEach(gId => {
      const ads = adsByGroup[gId] || [];
      ads.forEach(a => adIds.add(a.id));
    });
    
    return {
      companies: companyCount,
      groups: groupIds.size,
      ads: adIds.size,
      groupIds,
      adIds
    };
  }, [selectedCompanyIds, groupsByCompany, adsByGroup]);

  // Фильтрованные группы и объявления (если выбраны компании)
  // Отфильтрованные и отсортированные группы
  const filteredGroups = useMemo(() => {
    let groups = [...vkGroups];
    const { field, dir } = groupsSorting;
    
    groups.sort((a, b) => {
      let aVal: any, bVal: any;
      
      if (field === "status") {
        const aModStatus = getGroupModerationStatus(a.id);
        const bModStatus = getGroupModerationStatus(b.id);
        const aInfo = getEntityStatusInfo(a.status, aModStatus);
        const bInfo = getEntityStatusInfo(b.status, bModStatus);
        // Инвертируем порядок сортировки статуса
        return dir === "asc" ? bInfo.sortOrder - aInfo.sortOrder : aInfo.sortOrder - bInfo.sortOrder;
      }
      
      if (field.startsWith("base.")) {
        const statField = field.replace("base.", "");
        const aStats = vkGroupsStats[a.id]?.base || {};
        const bStats = vkGroupsStats[b.id]?.base || {};
        aVal = parseFloat((aStats as any)[statField] || "0") || 0;
        bVal = parseFloat((bStats as any)[statField] || "0") || 0;
      } else if (field === "created") {
        aVal = a.created || "";
        bVal = b.created || "";
      } else if (field === "budget_limit_day") {
        aVal = a.budget_limit_day || 0;
        bVal = b.budget_limit_day || 0;
      } else if (field === "name") {
        aVal = (a.name || "").toLowerCase();
        bVal = (b.name || "").toLowerCase();
      } else if (field === "id") {
        aVal = a.id;
        bVal = b.id;
      } else if (field === "revenue") {
        aVal = getGroupRevenue(a.id);
        bVal = getGroupRevenue(b.id);
      } else if (field === "profit") {
        aVal = getGroupProfit(a.id);
        bVal = getGroupProfit(b.id);
      } else {
        aVal = (a as any)[field];
        bVal = (b as any)[field];
      }
      
      let cmp = 0;
      if (typeof aVal === "number" && typeof bVal === "number") {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }
      
      return dir === "asc" ? cmp : -cmp;
    });
    
    return groups;
  }, [vkGroups, vkGroupsStats, groupsSorting, adsByGroup, revenueData, vkAds]);

  // Отфильтрованные и отсортированные объявления
  const filteredAds = useMemo(() => {
    let ads = [...vkAds];
    const { field, dir } = adsSorting;
    
    ads.sort((a, b) => {
      let aVal: any, bVal: any;
      
      if (field === "moderation_status" || field === "status") {
        const aInfo = getEntityStatusInfo(a.status, a.moderation_status);
        const bInfo = getEntityStatusInfo(b.status, b.moderation_status);
        // Инвертируем порядок сортировки статуса
        return dir === "asc" ? bInfo.sortOrder - aInfo.sortOrder : aInfo.sortOrder - bInfo.sortOrder;
      }
      
      if (field.startsWith("base.")) {
        const statField = field.replace("base.", "");
        const aStats = vkAdsStats[a.id]?.base || {};
        const bStats = vkAdsStats[b.id]?.base || {};
        aVal = parseFloat((aStats as any)[statField] || "0") || 0;
        bVal = parseFloat((bStats as any)[statField] || "0") || 0;
      } else if (field === "created") {
        aVal = a.created || "";
        bVal = b.created || "";
      } else if (field === "name") {
        aVal = (a.name || "").toLowerCase();
        bVal = (b.name || "").toLowerCase();
      } else if (field === "id") {
        aVal = a.id;
        bVal = b.id;
      } else if (field === "revenue") {
        aVal = getAdRevenue(a.id);
        bVal = getAdRevenue(b.id);
      } else if (field === "profit") {
        aVal = getAdProfit(a.id);
        bVal = getAdProfit(b.id);
      } else {
        aVal = (a as any)[field];
        bVal = (b as any)[field];
      }
      
      let cmp = 0;
      if (typeof aVal === "number" && typeof bVal === "number") {
        cmp = aVal - bVal;
      } else {
        cmp = String(aVal).localeCompare(String(bVal));
      }
      
      return dir === "asc" ? cmp : -cmp;
    });
    
    return ads;
  }, [vkAds, vkAdsStats, adsSorting, revenueData]);

  // Totals для групп (только активные)
  const groupsTotals = useMemo(() => {
    let shows = 0, clicks = 0, goals = 0, spent = 0, budget = 0;
    for (const group of filteredGroups) {
      // Бюджет считаем только для групп с активными объявлениями
      if (group.status === "active") {
        const groupAds = adsByGroup[group.id] || [];
        const hasActiveAd = groupAds.some(a => a.status === "active" && a.moderation_status === "allowed");
        if (hasActiveAd) {
          budget += group.budget_limit_day || 0;
        }
      }
      const stats = vkGroupsStats[group.id];
      if (stats?.base) {
        shows += stats.base.shows || 0;
        clicks += stats.base.clicks || 0;
        goals += stats.base.goals || 0;
        spent += parseFloat(stats.base.spent || "0") || 0;
      }
    }
    const cpc = clicks > 0 ? spent / clicks : 0;
    const cpa = goals > 0 ? spent / goals : 0;
    return { shows, clicks, goals, spent, cpc, cpa, budget };
  }, [filteredGroups, vkGroupsStats, adsByGroup]);

  // Totals для объявлений
  const adsTotals = useMemo(() => {
    let shows = 0, clicks = 0, goals = 0, spent = 0;
    for (const ad of filteredAds) {
      const stats = vkAdsStats[ad.id];
      if (stats?.base) {
        shows += stats.base.shows || 0;
        clicks += stats.base.clicks || 0;
        goals += stats.base.goals || 0;
        spent += parseFloat(stats.base.spent || "0") || 0;
      }
    }
    const cpc = clicks > 0 ? spent / clicks : 0;
    const cpa = goals > 0 ? spent / goals : 0;
    return { shows, clicks, goals, spent, cpc, cpa };
  }, [filteredAds, vkAdsStats]);

  // ----------- Подсчет групп пресета ------------
  const calcFastPresetGroupsCount = (preset: Preset): number => {
    if (!preset?.fastPreset) return preset?.groups?.length || 0;
    
    const groups = preset.groups || [];
    const ads = preset.ads || [];
    
    let totalGroups = 0;
    
    for (const g of groups) {
      const containers = g.containers || [];
      const containerCount = containers.length || 1; // если нет контейнеров — считаем как 1
      
      let creativesCount = 0;
      for (const ad of ads) {
        creativesCount += (ad.videoIds || []).length;
        creativesCount += (ad.imageIds || []).length;
      }
      
      totalGroups += creativesCount * containerCount;
    }
    
    return totalGroups;
  };
  // ----------- PIXEL -----------
  const [sitePixels, setSitePixels] = useState<SitePixel[]>([]);
  // ----------- TEXT -----------
  const [textSets, setTextSets] = useState<TextSet[]>([]);
  // LOGO
  const [logo, setLogo] = useState<LogoMeta>(null);
  const [logoLoading, setLogoLoading] = useState(false);
  
  useEffect(() => {
    if (!logo?.id || !presetDraft) return;
    // Подставляем logoId только в те объявления, где он ещё пустой
    setPresetDraft(prev => (prev ? applyDefaultLogoToDraft(prev, logo.id) : prev));
  }, [logo?.id]); // ← привязано к появлению/смене логотипа

  // для анимации загрузки креативов
  const [uploadingCount, setUploadingCount] = useState(0);

    // ---- кроп картинок при загрузке ----
  type CropTask = { file: File; setId: string };

  const [, setCropQueue] = useState<CropTask[]>([]);
  const [currentCropTask, setCurrentCropTask] = useState<CropTask | null>(null);
  const [cropModalOpen, setCropModalOpen] = useState(false);
  const [cropPreviewUrl, setCropPreviewUrl] = useState<string | null>(null);
  const [cropFormatId, setCropFormatId] = useState<CropFormatId>("600x600");
  const [cropRect, setCropRect] = useState<{ x: number; y: number; width: number; height: number }>({
    x: 0,
    y: 0,
    width: 0,
    height: 0,
  });
  const cropInnerRef = React.useRef<HTMLDivElement | null>(null);
  const [imgBox, setImgBox] = useState({ left: 0, top: 0, width: 0, height: 0 });
  const cropImgRef = React.useRef<HTMLImageElement | null>(null);
  const cropDragState = React.useRef<{
    active: boolean;
    startX: number;
    startY: number;
    startRect: { x: number; y: number };
  } | null>(null);

  // PIXEL

  const [pixelDialog, setPixelDialog] = useState<{
    open: boolean;
    domain: string;
    pixel: string;
    resolve?: (val: { domain: string; pixel: string } | null) => void;
  }>({
    open: false,
    domain: "",
    pixel: "",
  });

  const askPixelInput = (initial?: { domain?: string; pixel?: string }) => {
    return new Promise<{ domain: string; pixel: string } | null>((resolve) => {
      setPixelDialog({
        open: true,
        domain: initial?.domain ?? "",
        pixel: initial?.pixel ?? "",
        resolve,
      });
    });
  };

  const closePixelDialog = (val: { domain: string; pixel: string } | null) => {
    pixelDialog.resolve?.(val);
    setPixelDialog({ open: false, domain: "", pixel: "", resolve: undefined });
  };

  // -------- Confirm Dialog --------
  const [confirmDialog, setConfirmDialog] = useState<{
    open: boolean;
    msg: string;
    resolve?: (val: boolean) => void;
  }>({
    open: false,
    msg: ""
  });

  const [noCabinetsWarning, setNoCabinetsWarning] = useState(false);

  const askConfirm = (msg: string): Promise<boolean> => {
    return new Promise((resolve) => {
      setConfirmDialog({
        open: true,
        msg,
        resolve
      });
    });
  };

  const openContextMenuForItem = (
    e: React.MouseEvent,
    setId: string,
    itemId: string
  ) => {
    e.preventDefault();
    setContextMenu({
      visible: true,
      x: e.clientX,
      y: e.clientY,
      setId,
      itemId,
    });
  };

  useEffect(() => {
    const onClick = () => setContextMenu(null);
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setContextMenu(null);
    };
    window.addEventListener("click", onClick);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("click", onClick);
      window.removeEventListener("keydown", onKey);
    };
  }, []);

  const closeConfirm = (result: boolean) => {
    if (confirmDialog.resolve) confirmDialog.resolve(result);
    setConfirmDialog({ open: false, msg: "", resolve: undefined });
  };

  const handleRehash = async () => {
    if (!contextMenu || !userId) return;
    const { setId, itemId } = contextMenu;
    setContextMenu(null);

    if (!(await askConfirm("Перезагрузить видео и поменять хэш во всех пресетах?"))) {
      return;
    }

    try {
      const resp = await fetchSecured(`${API_BASE}/creative/rehash`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          setId,
          itemId,
        }),
      });

      const json = await resp.json();
      if (!resp.ok || json.status !== "ok") {
        throw new Error(json.error || json.detail || "Ошибка смены хэша");
      }

      // Перечитаем креативы и пресеты для текущего кабинета
      try {
        const cResp = await fetchSecured(
          `${API_BASE}/creatives/get?user_id=${encodeURIComponent(
            userId
          )}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        const cJson = await cResp.json();
        setCreativeSets(cJson.creatives || []);

        const pResp = await fetchSecured(
          `${API_BASE}/preset/list?user_id=${encodeURIComponent(
            userId
          )}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        const pJson = await pResp.json();
        setPresets(Array.isArray(pJson.presets) ? pJson.presets : []);
      } catch (e) {
        console.warn("Reload after rehash failed", e);
      }

      showPopup("Хэш видео обновлён");
    } catch (e: any) {
      console.error(e);
      showPopup(e.message || "Ошибка смены хэша");
    }
  };

  const [activeTab, setActiveTab] = useState<TabId>("campaigns");
  const [view, setView] = useState<View>({ type: "home" });

  const [sidebarOpen, setSidebarOpen] = useState(false);


  const closeT = React.useRef<number | null>(null);

  const openSidebar = () => {
    if (closeT.current) {
      window.clearTimeout(closeT.current);
      closeT.current = null;
    }
    setSidebarOpen(true);
  };

  const scheduleCloseSidebar = () => {
    if (closeT.current) window.clearTimeout(closeT.current);
    closeT.current = window.setTimeout(() => {
      setSidebarOpen(false);
      closeT.current = null;
    }, 180); // можно 0/120/180 как нравится
  };

  useEffect(() => {
    return () => {
      if (closeT.current) window.clearTimeout(closeT.current);
    };
  }, []);

  const [cabinets, setCabinets] = useState<Cabinet[]>([]);
  const [selectedCabinetId, setSelectedCabinetId] = useState<string>("all");
  const [settingsLoaded, setSettingsLoaded] = useState(false);

  const [tgInitData, setTgInitData] = useState<string>(""); // <-- добавь
  const authReady = useMemo(() => {
    // demo_user разрешаем сразу
    if (userId === "demo_user") return true;
    return !!tgInitData;
  }, [userId, tgInitData]);

  // ===== Lead Forms =====
  const [leadForms, setLeadForms] = useState<{ id: string; name: string }[]>([]);

  // ===== Trigger Presets for preset editor =====
  const [triggerPresets, setTriggerPresets] = useState<
    { id: string; name: string }[]
  >([]);

  const [presets, setPresets] = useState<
    { preset_id: string; data: Preset; created_at?: string }[]
  >([]);
  const [presetDraft, setPresetDraft] = useState<Preset | null>(null);
  const [selectedStructure, setSelectedStructure] = useState<{
    type: "company" | "group" | "ad";
    index?: number;
  }>({ type: "company" });

  const [creativeSets, setCreativeSets] = useState<CreativeSet[]>([]);
  const [selectedCreativeSetId, setSelectedCreativeSetId] =
    useState<string | null>(null);
  const [videoPicker, setVideoPicker] = useState<{
    open: boolean;
    adId: string | null;
  }>({ open: false, adId: null });


  // ===== Video picker perf: lazy render (ALL sets opened) =====
  const [pickerLimitBySet, setPickerLimitBySet] = useState<Record<string, number>>({});
  const drawerScrollRef = React.useRef<HTMLDivElement | null>(null);

  const PICKER_CHUNK = 20;

  // храним sentinel для каждого набора
  const pickerSentinelsRef = React.useRef<Map<string, HTMLDivElement>>(new Map());
  const pickerIORef = React.useRef<IntersectionObserver | null>(null);

  // callback-ref для div sentinel конкретного набора
  const registerPickerSentinel = React.useCallback(
    (setId: string) => (el: HTMLDivElement | null) => {
      const map = pickerSentinelsRef.current;
      const prevEl = map.get(setId);

      // если был старый элемент — разнаблюдаем
      if (prevEl && pickerIORef.current) pickerIORef.current.unobserve(prevEl);

      if (el) {
        map.set(setId, el);
        // если observer уже создан — начинаем наблюдать сразу
        if (pickerIORef.current) pickerIORef.current.observe(el);
      } else {
        map.delete(setId);
      }
    },
    []
  );

  const loadLeadForms = async (force = false) => {
    if (!userId || !selectedCabinetId) return;

    const headers: Record<string, string> = {};
    if (tgInitData) {
      headers["x-tg-init-data"] = tgInitData;
    }

    try {
      // 1️⃣ Пытаемся взять локальный кеш
      if (!force) {
        try {
          const getUrl =
            `${API_BASE}/leadforms/get` +
            `?user_id=${encodeURIComponent(userId)}` +
            `&cabinet_id=${encodeURIComponent(selectedCabinetId)}`;

          const getResp = await fetch(getUrl, { headers });

          if (getResp.ok) {
            const getJson = await getResp.json();
            const list = Array.isArray(getJson?.leadforms)
              ? getJson.leadforms
              : [];

            if (list.length > 0) {
              setLeadForms(list);
              return;
            }
          }
        } catch (e) {
          console.warn("leadforms/get failed, fallback to fetch", e);
        }
      }

      // 2️⃣ Фоллбек — тянем из VK
      const fetchUrl =
        `${API_BASE}/vk/lead_forms/fetch` +
        `?user_id=${encodeURIComponent(userId)}` +
        `&cabinet_id=${encodeURIComponent(selectedCabinetId)}`;

      const fetchResp = await fetch(fetchUrl, { headers });

      if (!fetchResp.ok) {
        const text = await fetchResp.text();
        throw new Error(`vk fetch failed: ${fetchResp.status} ${text}`);
      }

      const fetchJson = await fetchResp.json();
      const list = Array.isArray(fetchJson?.leadforms)
        ? fetchJson.leadforms
        : [];

      setLeadForms(list);
    } catch (e) {
      console.warn("loadLeadForms failed", e);
      setLeadForms([]);
    }
  };



  // -------- SETTINGS TAB ---------
  // После других useState в App
  const [settingsTab, setSettingsTab] = useState<"general" | "notifications" | "autoReupload">("general");
  const [notifyOnError, setNotifyOnError] = useState<boolean>(true);
  const [notifyOnCreate, setNotifyOnCreate] = useState<boolean>(false);
  const [notifyOnReupload, setNotifyOnReupload] = useState<boolean>(false);
  
  // Авто-перезалив
  const [autoReuploadEnabled, setAutoReuploadEnabled] = useState<boolean>(false);
  const [deleteRejected, setDeleteRejected] = useState<boolean>(false);
  const [skipModerationFail, setSkipModerationFail] = useState<boolean>(false);
  const [reuploadTimeStart, setReuploadTimeStart] = useState<string>("09:00");
  const [reuploadTimeEnd, setReuploadTimeEnd] = useState<string>("21:00");

  // Загрузка настроек уведомлений и авто-перезалива
  useEffect(() => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    (async () => {
      try {
        const j = await apiJson(
          `${API_BASE}/notifications/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        setNotifyOnError(j?.notifyOnError !== false); // по умолчанию true
        setNotifyOnCreate(j?.notifyOnCreate === true);
        setNotifyOnReupload(j?.notifyOnReupload === true);
      } catch {
        setNotifyOnError(true);
        setNotifyOnCreate(false);
        setNotifyOnReupload(false);
      }
    })();
    
    // Загрузка настроек авто-перезалива
    (async () => {
      try {
        const j = await apiJson(
          `${API_BASE}/auto-reupload/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        setAutoReuploadEnabled(j?.enabled === true);
        setDeleteRejected(j?.deleteRejected === true);
        setSkipModerationFail(j?.skipModerationFail === true);
        setReuploadTimeStart(j?.timeStart || "09:00");
        setReuploadTimeEnd(j?.timeEnd || "21:00");
      } catch {
        setAutoReuploadEnabled(false);
        setDeleteRejected(false);
        setSkipModerationFail(false);
        setReuploadTimeStart("09:00");
        setReuploadTimeEnd("21:00");
      }
    })();
  }, [userId, selectedCabinetId]);

  // Сохранение настроек уведомлений
  const saveNotificationSettings = async (settings: {
    notifyOnError?: boolean;
    notifyOnCreate?: boolean;
    notifyOnReupload?: boolean;
  }) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    try {
      await fetchSecured(`${API_BASE}/notifications/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          notifyOnError: settings.notifyOnError ?? notifyOnError,
          notifyOnCreate: settings.notifyOnCreate ?? notifyOnCreate,
          notifyOnReupload: settings.notifyOnReupload ?? notifyOnReupload,
        }),
      });
      if (settings.notifyOnError !== undefined) setNotifyOnError(settings.notifyOnError);
      if (settings.notifyOnCreate !== undefined) setNotifyOnCreate(settings.notifyOnCreate);
      if (settings.notifyOnReupload !== undefined) setNotifyOnReupload(settings.notifyOnReupload);
    } catch (e) {
      console.error("Failed to save notification settings", e);
      showPopup("Ошибка сохранения настроек");
    }
  };
  
  // Сохранение настроек авто-перезалива
  const saveAutoReuploadSettings = async (settings: {
    enabled?: boolean;
    deleteRejected?: boolean;
    skipModerationFail?: boolean;
    timeStart?: string;
    timeEnd?: string;
  }) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    try {
      await fetchSecured(`${API_BASE}/auto-reupload/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          enabled: settings.enabled ?? autoReuploadEnabled,
          deleteRejected: settings.deleteRejected ?? deleteRejected,
          skipModerationFail: settings.skipModerationFail ?? skipModerationFail,
          timeStart: settings.timeStart ?? reuploadTimeStart,
          timeEnd: settings.timeEnd ?? reuploadTimeEnd,
        }),
      });
      if (settings.enabled !== undefined) setAutoReuploadEnabled(settings.enabled);
      if (settings.deleteRejected !== undefined) setDeleteRejected(settings.deleteRejected);
      if (settings.skipModerationFail !== undefined) setSkipModerationFail(settings.skipModerationFail);
      if (settings.timeStart !== undefined) setReuploadTimeStart(settings.timeStart);
      if (settings.timeEnd !== undefined) setReuploadTimeEnd(settings.timeEnd);
    } catch (e) {
      console.error("Failed to save auto-reupload settings", e);
      showPopup("Ошибка сохранения настроек авто-перезалива");
    }
  };

  useEffect(() => {
    if (!presetDraft) return;

    if (presetDraft.company?.targetAction === "leadads") {
      loadLeadForms(false);
    }
  }, [presetDraft?.company?.targetAction, userId, selectedCabinetId]);


  // === SAVE COMPANIES SETTINGS ===
  useEffect(() => {
    localStorage.setItem("companiesSorting", JSON.stringify(companiesSorting));
  }, [companiesSorting]);

  useEffect(() => {
    localStorage.setItem("companiesColumns", JSON.stringify(companiesColumns));
  }, [companiesColumns]);

  useEffect(() => {
    localStorage.setItem("groupsSorting", JSON.stringify(groupsSorting));
  }, [groupsSorting]);

  useEffect(() => {
    localStorage.setItem("groupsColumns", JSON.stringify(groupsColumns));
  }, [groupsColumns]);

  useEffect(() => {
    localStorage.setItem("adsSorting", JSON.stringify(adsSorting));
  }, [adsSorting]);

  useEffect(() => {
    localStorage.setItem("adsColumns", JSON.stringify(adsColumns));
  }, [adsColumns]);

  // Кэш для таблиц VK (только списки, НЕ статистика - статистика зависит от дат)
  const [vkDataCache, setVkDataCache] = useState<{
    companies: { data: VkCompany[]; total: number; timestamp: number } | null;
    groups: { data: VkGroup[]; timestamp: number } | null;
    ads: { data: VkAd[]; timestamp: number } | null;
  }>({ companies: null, groups: null, ads: null });

  const CACHE_TTL = 15 * 60 * 1000; // 15 минут

  // Заменить fetchVkCompanies на:
  const fetchVkCompanies = async (forceRefresh = false) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;

    // Проверяем кэш ТОЛЬКО для списка компаний
    const cached = vkDataCache.companies;
    if (!forceRefresh && cached && Date.now() - cached.timestamp < CACHE_TTL) {
      setVkCompanies(cached.data);
      setVkCompaniesTotal(cached.total);
      // Статистику ВСЕГДА загружаем заново (зависит от дат)
      if (cached.data.length > 0) {
        await fetchVkCompaniesStats(cached.data.map(it => it.id));
      }
      return;
    }

    setVkCompaniesLoading(true);

    try {
      const firstPage = await apiJson(
        `${API_BASE}/vk/ad_plans/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=0`
      );
      
      let items: VkCompany[] = firstPage?.items || [];
      const count = firstPage?.count || 0;
      
      if (count > 200) {
        const pages = Math.ceil(count / 200);
        for (let page = 1; page < pages && page < 10; page++) {
          const nextPage = await apiJson(
            `${API_BASE}/vk/ad_plans/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=${page * 200}`
          );
          items = [...items, ...(nextPage?.items || [])];
        }
      }
      
      setVkCompanies(items);
      setVkCompaniesTotal(count);

      // Сохраняем в кэш ТОЛЬКО список
      setVkDataCache(prev => ({
        ...prev,
        companies: { data: items, total: count, timestamp: Date.now() }
      }));

      // Загружаем статистику
      if (items.length > 0) {
        await fetchVkCompaniesStats(items.map(it => it.id));
      }

    } catch (e) {
      console.error("fetchVkCompanies error:", e);
      showPopup("Ошибка загрузки компаний");
    } finally {
      setVkCompaniesLoading(false);
    }
  };

  const fetchVkCompaniesStats = async (ids: number[], dateFrom?: string, dateTo?: string) => {
    const from = dateFrom || companiesDateFrom;
    const to = dateTo || companiesDateTo;
    if (!userId || !selectedCabinetId || selectedCabinetId === "all" || ids.length === 0) return;

    const statsMap: Record<number, VkCompanyStats> = {};
    
    const chunkSize = 200;
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      const idsStr = chunk.join(",");

      let attempts = 0;
      const maxAttempts = 3;
      
      while (attempts < maxAttempts) {
        try {
          const response = await apiJson(
            `${API_BASE}/vk/statistics/ad_plans?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&ids=${idsStr}&date_from=${from}&date_to=${to}&metrics=base&limit=200`
          );

          for (const stat of (response?.items || [])) {
            statsMap[stat.id] = stat;
          }
          break;
        } catch (e: any) {
          attempts++;
          const errorMsg = e?.message || "";
          
          if (errorMsg.includes("429") && attempts < maxAttempts) {
            console.log(`Rate limit hit, waiting before retry ${attempts}/${maxAttempts}...`);
            await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
            continue;
          }
          
          console.error("fetchVkCompaniesStats error:", e);
          break;
        }
      }
    }
    
    setVkCompaniesStats(statsMap);
    // НЕ сохраняем статистику в кэш - она зависит от дат
  };

  const fetchVkGroups = async (forceRefresh = false) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;

    const cached = vkDataCache.groups;
    if (!forceRefresh && cached && Date.now() - cached.timestamp < CACHE_TTL) {
      setVkGroups(cached.data);
      if (cached.data.length > 0) {
        await fetchVkGroupsStats(cached.data.map(it => it.id));
      }
      return;
    }

    setVkGroupsLoading(true);

    try {
      const firstPage = await apiJson(
        `${API_BASE}/vk/ad_groups/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=0`
      );
      
      let items: VkGroup[] = firstPage?.items || [];
      const count = firstPage?.count || 0;
      
      if (count > 200) {
        const pages = Math.ceil(count / 200);
        for (let page = 1; page < pages && page < 10; page++) {
          const nextPage = await apiJson(
            `${API_BASE}/vk/ad_groups/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=${page * 200}`
          );
          items = [...items, ...(nextPage?.items || [])];
        }
      }
      
      setVkGroups(items);
      
      setVkDataCache(prev => ({
        ...prev,
        groups: { data: items, timestamp: Date.now() }
      }));
      
      if (items.length > 0) {
        await fetchVkGroupsStats(items.map(it => it.id));
      }

    } catch (e) {
      console.error("fetchVkGroups error:", e);
      showPopup("Ошибка загрузки групп");
    } finally {
      setVkGroupsLoading(false);
    }
  };

  const fetchVkGroupsStats = async (ids: number[], dateFrom?: string, dateTo?: string) => {
    const from = dateFrom || companiesDateFrom;
    const to = dateTo || companiesDateTo;
    if (!userId || !selectedCabinetId || selectedCabinetId === "all" || ids.length === 0) return;

    const statsMap: Record<number, VkGroupStats> = {};
    
    const chunkSize = 200;
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      const idsStr = chunk.join(",");

      let attempts = 0;
      const maxAttempts = 3;
      
      while (attempts < maxAttempts) {
        try {
          const response = await apiJson(
            `${API_BASE}/vk/statistics/ad_groups?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&ids=${idsStr}&date_from=${from}&date_to=${to}&metrics=base&limit=200`
          );

          for (const stat of (response?.items || [])) {
            statsMap[stat.id] = stat;
          }
          break;
        } catch (e: any) {
          attempts++;
          const errorMsg = e?.message || "";
          
          if (errorMsg.includes("429") && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
            continue;
          }
          
          console.error("fetchVkGroupsStats error:", e);
          break;
        }
      }
    }
    
    setVkGroupsStats(statsMap);
  };

  // === FETCH VK ADS ===
  const fetchVkAds = async (forceRefresh = false) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;

    const cached = vkDataCache.ads;
    if (!forceRefresh && cached && Date.now() - cached.timestamp < CACHE_TTL) {
      setVkAds(cached.data);
      if (cached.data.length > 0) {
        await fetchVkAdsStats(cached.data.map(it => it.id));
      }
      return;
    }

    setVkAdsLoading(true);

    try {
      const firstPage = await apiJson(
        `${API_BASE}/vk/banners/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=0`
      );
      
      let items: VkAd[] = firstPage?.items || [];
      const count = firstPage?.count || 0;
      
      if (count > 200) {
        const pages = Math.ceil(count / 200);
        for (let page = 1; page < pages && page < 10; page++) {
          const nextPage = await apiJson(
            `${API_BASE}/vk/banners/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&limit=200&offset=${page * 200}`
          );
          items = [...items, ...(nextPage?.items || [])];
        }
      }
      
      setVkAds(items);
      
      setVkDataCache(prev => ({
        ...prev,
        ads: { data: items, timestamp: Date.now() }
      }));
      
      if (items.length > 0) {
        await fetchVkAdsStats(items.map(it => it.id));
      }

    } catch (e) {
      console.error("fetchVkAds error:", e);
      showPopup("Ошибка загрузки объявлений");
    } finally {
      setVkAdsLoading(false);
    }
  };

  const fetchVkAdsStats = async (ids: number[], dateFrom?: string, dateTo?: string) => {
    const from = dateFrom || companiesDateFrom;
    const to = dateTo || companiesDateTo;
    if (!userId || !selectedCabinetId || selectedCabinetId === "all" || ids.length === 0) return;

    const statsMap: Record<number, VkAdStats> = {};
    
    const chunkSize = 200;
    for (let i = 0; i < ids.length; i += chunkSize) {
      const chunk = ids.slice(i, i + chunkSize);
      const idsStr = chunk.join(",");

      let attempts = 0;
      const maxAttempts = 3;
      
      while (attempts < maxAttempts) {
        try {
          const response = await apiJson(
            `${API_BASE}/vk/statistics/banners?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}&ids=${idsStr}&date_from=${from}&date_to=${to}&metrics=base&limit=200`
          );

          for (const stat of (response?.items || [])) {
            statsMap[stat.id] = stat;
          }
          break;
        } catch (e: any) {
          attempts++;
          const errorMsg = e?.message || "";
          
          if (errorMsg.includes("429") && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 1000 * attempts));
            continue;
          }
          
          console.error("fetchVkAdsStats error:", e);
          break;
        }
      }
    }
    
    setVkAdsStats(statsMap);
  };

  // Сбрасываем кэш при смене кабинета
  useEffect(() => {
    setVkDataCache({ companies: null, groups: null, ads: null });
  }, [selectedCabinetId]);

  // === SUB1 & REVENUE ===
  const fetchUserSettings = async () => {
    if (!userId) return;
    try {
      const res = await apiJson(`${API_BASE}/user/settings?user_id=${encodeURIComponent(userId)}`);
      setUserSub1(res?.sub1 || []);
    } catch (e) {
      console.error("fetchUserSettings error:", e);
    }
  };

  const saveUserSub1 = async (sub1List: string[]) => {
    if (!userId) return;
    try {
      await fetchSecured(`${API_BASE}/user/settings/sub1?user_id=${encodeURIComponent(userId)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(sub1List),
      });
      setUserSub1(sub1List);
      // После сохранения перезагружаем данные revenue
      fetchRevenueData(sub1List);
    } catch (e) {
      console.error("saveUserSub1 error:", e);
    }
  };

  const fetchRevenueData = async (sub1List?: string[]) => {
    const subs = sub1List || userSub1;
    if (subs.length === 0 || !companiesDateFrom || !companiesDateTo) {
      setRevenueData({});
      return;
    }
    try {
      const res = await apiJson(
        `${API_BASE}/leads/revenue?sub1=${encodeURIComponent(subs.join(","))}&date_from=${companiesDateFrom}&date_to=${companiesDateTo}`
      );
      setRevenueData(res?.revenue || {});
    } catch (e) {
      console.error("fetchRevenueData error:", e);
      setRevenueData({});
    }
  };


  // Загрузка данных при переключении вкладок и сортировки
  useEffect(() => {
    if (activeTab === "campaigns" && selectedCabinetId && selectedCabinetId !== "all") {
      if (campaignsSubTab === "companies") {
        fetchVkCompanies();
        fetchVkGroups();
        fetchVkAds();
      } else if (companiesViewTab === "campaigns") {
        fetchVkCompanies();
      } else if (companiesViewTab === "groups") {
        fetchVkGroups();
      } else if (companiesViewTab === "ads") {
        fetchVkAds();
      }
    }
  }, [activeTab, campaignsSubTab, companiesViewTab, selectedCabinetId, companiesDateFrom, companiesDateTo]);

  // Загрузка настроек пользователя (sub1)
  useEffect(() => {
    if (userId) {
      fetchUserSettings();
    }
  }, [userId]);

  // Обновление только статистики при смене дат (без перезагрузки списков)
  useEffect(() => {
    if (activeTab === "campaigns" && selectedCabinetId && selectedCabinetId !== "all" && campaignsSubTab === "companies") {
      const updateStats = async () => {
        // Показываем loading только если есть данные
        const hasCompanies = vkCompanies.length > 0;
        const hasGroups = vkGroups.length > 0;
        const hasAds = vkAds.length > 0;
        
        if (hasCompanies) setVkCompaniesLoading(true);
        if (hasGroups) setVkGroupsLoading(true);
        if (hasAds) setVkAdsLoading(true);
        
        try {
          // Перезагружаем ТОЛЬКО статистику с новыми датами
          const promises: Promise<void>[] = [];
          
          if (hasCompanies) {
            promises.push(fetchVkCompaniesStats(vkCompanies.map(c => c.id), companiesDateFrom, companiesDateTo));
          }
          if (hasGroups) {
            promises.push(fetchVkGroupsStats(vkGroups.map(g => g.id), companiesDateFrom, companiesDateTo));
          }
          if (hasAds) {
            promises.push(fetchVkAdsStats(vkAds.map(a => a.id), companiesDateFrom, companiesDateTo));
          }
          
          await Promise.all(promises);
        } finally {
          setVkCompaniesLoading(false);
          setVkGroupsLoading(false);
          setVkAdsLoading(false);
        }
      };
      
      updateStats();
    }
  }, [companiesDateFrom, companiesDateTo]);

  // Загрузка revenue при изменении sub1 или дат
  useEffect(() => {
    if (userSub1.length > 0 && companiesDateFrom && companiesDateTo) {
      fetchRevenueData();
    }
  }, [userSub1, companiesDateFrom, companiesDateTo]);

  // Обновление истории при переключении на вкладку
  useEffect(() => {
    if (activeTab === "history" && userId && selectedCabinetId && selectedCabinetId !== "all") {
      // Перезагружаем историю
      (async () => {
        try {
          const hJson = await apiJson(
            `${API_BASE}/history/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
          );
          setHistory(Array.isArray(hJson?.items) ? hJson.items : Array.isArray(hJson) ? hJson : []);
        } catch {
          setHistory([]);
        }
      })();
    }
  }, [activeTab]);

  // === COLUMN HANDLERS ===
  const handleColumnDragStart = (e: React.DragEvent, columnId: string) => {
    setDraggedColumnId(columnId);
    e.dataTransfer.effectAllowed = "move";
  };

  const handleColumnDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
  };

  const handleColumnDrop = (e: React.DragEvent, targetColumnId: string) => {
    e.preventDefault();
    if (!draggedColumnId || draggedColumnId === targetColumnId) {
      setDraggedColumnId(null);
      return;
    }

    setCompaniesColumns(prev => {
      const newColumns = [...prev];
      const draggedIdx = newColumns.findIndex(c => c.id === draggedColumnId);
      const targetIdx = newColumns.findIndex(c => c.id === targetColumnId);

      if (draggedIdx === -1 || targetIdx === -1) return prev;

      const [dragged] = newColumns.splice(draggedIdx, 1);
      newColumns.splice(targetIdx, 0, dragged);

      return newColumns;
    });

    setDraggedColumnId(null);
  };

  const handleSortClick = (column: CompaniesColumnConfig) => {
    if (!column.sortable || !column.sortField) return;

    setCompaniesSorting(prev => {
      if (prev.field === column.sortField) {
        return { field: column.sortField, dir: prev.dir === "asc" ? "desc" : "asc" };
      }
      return { field: column.sortField!, dir: "desc" };
    });
  };

  // === GROUPS COLUMN HANDLERS ===
  const handleGroupColumnDragStart = (e: React.DragEvent, columnId: string) => {
    setDraggedGroupColumnId(columnId);
    e.dataTransfer.effectAllowed = "move";
  };

  const handleGroupColumnDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
  };

  const handleGroupColumnDrop = (e: React.DragEvent, targetColumnId: string) => {
    e.preventDefault();
    if (!draggedGroupColumnId || draggedGroupColumnId === targetColumnId) {
      setDraggedGroupColumnId(null);
      return;
    }

    setGroupsColumns(prev => {
      const newColumns = [...prev];
      const draggedIdx = newColumns.findIndex(c => c.id === draggedGroupColumnId);
      const targetIdx = newColumns.findIndex(c => c.id === targetColumnId);

      if (draggedIdx === -1 || targetIdx === -1) return prev;

      const [dragged] = newColumns.splice(draggedIdx, 1);
      newColumns.splice(targetIdx, 0, dragged);

      return newColumns;
    });

    setDraggedGroupColumnId(null);
  };

  const handleGroupSortClick = (column: GroupsColumnConfig) => {
    if (!column.sortable || !column.sortField) return;

    setGroupsSorting(prev => {
      if (prev.field === column.sortField) {
        return { field: column.sortField, dir: prev.dir === "asc" ? "desc" : "asc" };
      }
      return { field: column.sortField!, dir: "desc" };
    });
  };

  // === ADS COLUMN HANDLERS ===
  const handleAdColumnDragStart = (e: React.DragEvent, columnId: string) => {
    setDraggedAdColumnId(columnId);
    e.dataTransfer.effectAllowed = "move";
  };

  const handleAdColumnDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
  };

  const handleAdColumnDrop = (e: React.DragEvent, targetColumnId: string) => {
    e.preventDefault();
    if (!draggedAdColumnId || draggedAdColumnId === targetColumnId) {
      setDraggedAdColumnId(null);
      return;
    }

    setAdsColumns(prev => {
      const newColumns = [...prev];
      const draggedIdx = newColumns.findIndex(c => c.id === draggedAdColumnId);
      const targetIdx = newColumns.findIndex(c => c.id === targetColumnId);

      if (draggedIdx === -1 || targetIdx === -1) return prev;

      const [dragged] = newColumns.splice(draggedIdx, 1);
      newColumns.splice(targetIdx, 0, dragged);

      return newColumns;
    });

    setDraggedAdColumnId(null);
  };

  const handleAdSortClick = (column: AdsColumnConfig) => {
    if (!column.sortable || !column.sortField) return;

    setAdsSorting(prev => {
      if (prev.field === column.sortField) {
        return { field: column.sortField, dir: prev.dir === "asc" ? "desc" : "asc" };
      }
      return { field: column.sortField!, dir: "desc" };
    });
  };

  // Toggle company status (active/blocked)
  const toggleCompanyStatus = async (companyId: number, currentStatus: string) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    const newStatus = currentStatus === "active" ? "blocked" : "active";
    
    setCompanyTogglingIds(prev => new Set(prev).add(companyId));
    
    try {
      const cab = cabinets.find(c => String(c.id) === String(selectedCabinetId));
      if (!cab?.token) throw new Error("No token");

      await fetchSecured(`${API_BASE}/vk/ad_plans/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          companyId,
          status: newStatus
        })
      });

      // Update local state
      setVkCompanies(prev => prev.map(c => 
        c.id === companyId ? { ...c, status: newStatus } : c
      ));
      
      // Clear cache
      const cacheKey = `vkCompanies_${selectedCabinetId}_${companiesSorting.field}_${companiesSorting.dir}`;
      sessionStorage.removeItem(cacheKey);
      
    } catch (e: any) {
      console.error("Toggle company status error:", e);
      showPopup("Ошибка изменения статуса");
    } finally {
      setCompanyTogglingIds(prev => {
        const next = new Set(prev);
        next.delete(companyId);
        return next;
      });
    }
  };

  // Обработчик изменения дат
  const handleDateChange = (from: string, to: string) => {
    setCompaniesDateFrom(from);
    setCompaniesDateTo(to);
  };

  // Toggle group status
  const toggleGroupStatus = async (groupId: number, currentStatus: string) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    const newStatus = currentStatus === "active" ? "blocked" : "active";
    
    setGroupTogglingIds(prev => new Set(prev).add(groupId));
    
    try {
      await fetchSecured(`${API_BASE}/vk/ad_groups/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          groupId,
          status: newStatus
        })
      });

      setVkGroups(prev => prev.map(g => 
        g.id === groupId ? { ...g, status: newStatus } : g
      ));
    } catch (e: any) {
      console.error("Toggle group status error:", e);
      showPopup("Ошибка изменения статуса группы");
    } finally {
      setGroupTogglingIds(prev => {
        const next = new Set(prev);
        next.delete(groupId);
        return next;
      });
    }
  };

  // Toggle ad status
  const toggleAdStatus = async (adId: number, currentStatus: string) => {
    if (!userId || !selectedCabinetId || selectedCabinetId === "all") return;
    
    const newStatus = currentStatus === "active" ? "blocked" : "active";
    
    setAdTogglingIds(prev => new Set(prev).add(adId));
    
    try {
      await fetchSecured(`${API_BASE}/vk/banners/status`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          bannerId: adId,
          status: newStatus
        })
      });

      setVkAds(prev => prev.map(a => 
        a.id === adId ? { ...a, status: newStatus } : a
      ));
    } catch (e: any) {
      console.error("Toggle ad status error:", e);
      showPopup("Ошибка изменения статуса объявления");
    } finally {
      setAdTogglingIds(prev => {
        const next = new Set(prev);
        next.delete(adId);
        return next;
      });
    }
  };

  // Select/deselect group
  const toggleGroupSelect = (id: number) => {
    setSelectedGroupIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAllGroups = () => {
    if (selectedGroupIds.size > 0) {
      setSelectedGroupIds(new Set());
    } else {
      setSelectedGroupIds(new Set(filteredGroups.map(g => g.id)));
    }
  };

  // Select/deselect ad
  const toggleAdSelect = (id: number) => {
    setSelectedAdIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAllAds = () => {
    if (selectedAdIds.size > 0) {
      setSelectedAdIds(new Set());
    } else {
      setSelectedAdIds(new Set(filteredAds.map(a => a.id)));
    }
  };

  // Select/deselect company
  const toggleCompanySelect = (id: number) => {
    setSelectedCompanyIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const toggleSelectAllCompanies = () => {
    // Если есть выбранные — всегда очищаем
    if (selectedCompanyIds.size > 0) {
      setSelectedCompanyIds(new Set());
    } else {
      setSelectedCompanyIds(new Set(vkCompanies.map(c => c.id)));
    }
  };

  React.useEffect(() => {
    if (!videoPicker.open) return;

    setPickerLimitBySet((prev) => {
      const next = { ...prev };
      creativeSets.forEach((s) => {
        if (next[s.id] == null) next[s.id] = PICKER_CHUNK;
      });
      return next;
    });
  }, [videoPicker.open, creativeSets]);

  React.useEffect(() => {
    if (!videoPicker.open) return;

    const root = drawerScrollRef.current;
    if (!root) return;

    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (!entry.isIntersecting) continue;

          const setId = (entry.target as HTMLElement).dataset.setid;
          if (!setId) continue;

          setPickerLimitBySet((prev) => {
            const cur = prev[setId] ?? PICKER_CHUNK;
            const set = creativeSets.find((s) => s.id === setId);
            const max = set ? set.items.length : cur + PICKER_CHUNK;
            const nextVal = Math.min(cur + PICKER_CHUNK, max);

            if (nextVal === cur) return prev;
            return { ...prev, [setId]: nextVal };
          });
        }
      },
      { root, rootMargin: "200px", threshold: 0.01 }
    );

    pickerIORef.current = io;

    // подписываем все уже зарегистрированные sentinel’ы
    pickerSentinelsRef.current.forEach((el) => io.observe(el));

    return () => {
      io.disconnect();
      pickerIORef.current = null;
    };
  }, [videoPicker.open, creativeSets]);

  const [audiences, setAudiences] = useState<Audience[]>([]);
  const [audTab, setAudTab] = useState<"vk" | "lists" | "templates">("vk");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [history, setHistory] = useState<HistoryItem[]>([]);
  const [openedHistoryIds, setOpenedHistoryIds] = useState<Record<string, boolean>>({});

  // ----------------- Theme -----------------
  useEffect(() => {
    const stored = localStorage.getItem("auto_ads_theme") as Theme | null;
    const initial: Theme =
      stored === "light" || stored === "dark" ? stored : "dark";
  
    setTheme(initial);
    document.documentElement.setAttribute("data-theme", initial);
    localStorage.setItem("auto_ads_theme", initial);
  }, []);

  const toggleTheme = () => {
    setTheme((prev) => {
      const next: Theme = prev === "light" ? "dark" : "light";
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("auto_ads_theme", next);
      return next;
    });
  };

  // ----------------- Mobile guard -----------------
  useEffect(() => {
    const update = () => {
      setIsMobile(window.innerWidth < 1024);
    };
    update();
    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, []);

  // ----------------- Telegram init -----------------
  useEffect(() => {
    const tg = window.Telegram?.WebApp;
    console.log("TG WebApp:", tg); // отладка

    if (!tg) {
      console.warn("⚠️ Telegram WebApp не найден — вход как demo_user");
      setUserId("demo_user");
      return;
    }

    tg.expand();
    try { tg.ready(); } catch {}

    const user = tg.initDataUnsafe?.user;
    console.log("TG User:", user);

    if (user?.id) setUserId(String(user.id));
    else setUserId("demo_user");

    // всегда пытаемся дождаться initData (для demo можно просто "demo")
    const tick = () => {
      const v = tg?.initData || "";
      if (v) setTgInitData(v);
      else window.setTimeout(tick, 50);
    };
    tick();

    if (!user?.id) setTgInitData("demo");
  }, []);


  // ----------------- Load settings & data -----------------
  // 1) Грузим settings один раз, выставляем selectedCabinetId
  useEffect(() => {
    if (!userId) return;
    (async () => {
      setLoading(true);
      try {
        const sJson = await apiJson(`${API_BASE}/settings/get?user_id=${encodeURIComponent(userId)}`);
        const settings = sJson.settings || {};

        const cabinetsFromSettings: Cabinet[] = settings.cabinets ?? [];
        setCabinets(cabinetsFromSettings);
        setNoCabinetsWarning(cabinetsFromSettings.length <= 1);

        const initialCabId = settings.selected_cabinet_id
          ? String(settings.selected_cabinet_id)
          : String(cabinetsFromSettings[0]?.id ?? "all");

        setSelectedCabinetId(initialCabId);
        setSettingsLoaded(true);
      } catch (e) {
        console.error(e);
        showPopup("Ошибка загрузки настроек");
      } finally {
        setLoading(false);
        setSettingsLoaded(true);
      }
    })();
  }, [userId]);

  // 2) Когда выбран кабинет — грузим всё остальное (с отменой запросов)
  useEffect(() => {
    if (!userId) return;
    if (!settingsLoaded) return;
    if (!selectedCabinetId || selectedCabinetId === "all") {
      return;
    }
    if (!authReady) return;

    const ac = new AbortController();
    const { signal } = ac;

    (async () => {
      setLoading(true);
      setError(null);

      const cabId = selectedCabinetId;
      const tasks = [
        (async () => {
          const tJson = await apiJson(
            `${API_BASE}/textsets/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          setTextSets(Array.isArray(tJson.textsets) ? tJson.textsets : []);
        })(),
      
        (async () => {
          const pResp = await fetchSecured(
            `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          const pJson = await pResp.json();
          setPresets(Array.isArray(pJson.presets) ? pJson.presets : []);
        
          // queue status (не критично)
          try {
            const map = await getQueueStatuses(userId, cabId);
            setQueueStatus(map);
          } catch {
            const fallback: Record<string,"active"|"deactive"> = {};
            (Array.isArray(pJson.presets) ? pJson.presets : []).forEach((p:any) => fallback[p.preset_id] = "active");
            setQueueStatus(fallback);
          }
        })(),

        // Загрузка trigger presets для dropdown в редакторе пресета
        (async () => {
          try {
            const tpResp = await fetchSecured(
              `${API_BASE}/trigger_presets/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
              { signal }
            );
            const tpJson = await tpResp.json();
            setTriggerPresets(
              Array.isArray(tpJson.trigger_presets)
                ? tpJson.trigger_presets.map((tp: any) => ({ id: tp.id, name: tp.name }))
                : []
            );
          } catch {
            setTriggerPresets([]);
          }
        })(),
      
        (async () => {
          const cResp = await fetchSecured(
            `${API_BASE}/creatives/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          const cJson = await cResp.json();
          setCreativeSets(cJson.creatives || []);
        })(),
      
        (async () => {
          const aResp = await fetchSecured(
            `${API_BASE}/audiences/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          const aJson = await aResp.json();
          setAudiences(aJson.audiences || []);
        })(),
      
        (async () => {
          try {
            const hJson = await apiJson(
              `${API_BASE}/history/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
              { signal }
            );
            setHistory(Array.isArray(hJson?.items) ? hJson.items : Array.isArray(hJson) ? hJson : []);
          } catch {
            setHistory([]);
          }
        })(),
      
        (async () => {
          const aaResp = await fetchSecured(
            `${API_BASE}/abstract_audiences/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          const aaJson = await aaResp.json();
          setAbstractAudiences(aaJson.audiences || []);
        })(),
      
        (async () => {
          try {
            const lgJson = await apiJson(
              `${API_BASE}/logo/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
              { signal }
            );
            setLogo(lgJson.logo || null);
          } catch {
            setLogo(null);
          }
        })(),
      ];
    
      const results = await Promise.allSettled(tasks);
    
      // логируем реальные падения, но не показываем “общую” ошибку на abort
      for (const r of results) {
        if (r.status === "rejected") {
          if (r.reason?.name === "AbortError") continue;
          console.warn("load failed:", r.reason);
        }
      }
    
      setLoading(false);
    })();

    // Отмена всех запросов при смене кабинета/размонтаже
    return () => ac.abort();
  }, [userId, selectedCabinetId, settingsLoaded, authReady]);

  // PIXEL
  useEffect(() => {
    if (!userId) return;

    if (!selectedCabinetId || selectedCabinetId === "all") {
      setSitePixels([]);
      return;
    }

    (async () => {
      try {
        const j = await apiJson(
          // лучше сделать единообразно с save:
          // `${API_BASE}/pixels/get?...`
          `${API_BASE}/pixels/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );

        setSitePixels(normalizeSitePixels(j?.pixels));
      } catch {
        setSitePixels([]);
      }
    })();
  }, [userId, selectedCabinetId]);

  // ----------------- Preset helpers -----------------

  const startNewPreset = () => {
    const preset: Preset = {
      company: {
        presetName: "",
        companyName: "",
        targetAction: "",
        trigger: "time",
        time: "",
        duplicates: 1,
        siteAction: "uss:success",
        sitePixel: "",
        url: "",
        bannerUrl: "",
      },
      groups: [
        {
          id: generateId(),
          groupName: "",
          bidStrategy: "min",
          maxCpa: "",
          budget: "600",
          regions: [188],
          gender: "male,female",
          age: "21-55",
          interests: [],
          audienceIds: [],
          audienceNames: [],
          abstractAudiences: [],
          placements: [],
          utm: ""  
        },
      ],
      ads: [
        {
          id: generateId(),
          adName: "",
          textSetId: null,
          newTextSetName: "",
          shortDescription: "",
          longDescription: "",
          button: "",
          buttonText: "",
          videoIds: [],
          imageIds: [],
          creativeSetIds: [],
          url: ""
        },
      ],
    };
    const withLogo = applyDefaultLogoToDraft(preset, logo?.id);
    setPresetDraft(withLogo);
    setSelectedStructure({ type: "company" });
    setView({ type: "presetEditor", presetId: undefined });
  };

  const startFastPreset = () => {
    const preset: Preset = {
      fastPreset: true,
      company: {
        presetName: "",
        companyName: "",
        targetAction: "",
        trigger: "time",
        time: "",
        duplicates: 1,
        siteAction: "uss:success",
        sitePixel: "",
        url: "",
        bannerUrl: "",
      },
      groups: [
        {
          id: generateId(),
          groupName: "",
          bidStrategy: "min",
          maxCpa: "",
          budget: "600",
          regions: [188],
          gender: "male,female",
          age: "21-55",
          interests: [],
          audienceIds: [],
          audienceNames: [],
          abstractAudiences: [],
          utm: "",
          placements: [],
          containers: []
        },
      ],
      ads: [
        {
          id: generateId(),
          adName: "",
          textSetId: null,
          newTextSetName: "",
          shortDescription: "",
          longDescription: "",
          button: "",
          buttonText: "",
          videoIds: [],
          imageIds: [],             // ←
          creativeSetIds: [],
          url: ""
        },
      ],
    };
    const withLogo = applyDefaultLogoToDraft(preset, logo?.id);
    setPresetDraft(withLogo);
    setSelectedStructure({ type: "company" });
    setView({ type: "presetEditor", presetId: undefined });
  };

  const openPreset = (presetId: string, data: Preset | any) => {
    const normalized = normalizePreset(data);
    const withLogo = applyDefaultLogoToDraft(normalized, logo?.id);
    setPresetDraft(withLogo);
    setSelectedStructure({ type: "company" });
    setView({ type: "presetEditor", presetId });
  };
  

  const cloneGroup = (index: number) => {
    if (!presetDraft) return;
    const group = presetDraft.groups[index];
    const ad = presetDraft.ads[index];

    const newGroup: PresetGroup = {
      ...group,
      id: generateId(),
    };

    const newAd: PresetAd = {
      ...ad,
      id: generateId(),
    };

    const groups = [...presetDraft.groups];
    const ads = [...presetDraft.ads];

    groups.splice(index + 1, 0, newGroup);
    ads.splice(index + 1, 0, newAd);

    setPresetDraft({ ...presetDraft, groups, ads });
  };

  const deleteGroup = (index: number) => {
    if (!presetDraft) return;
    if (presetDraft.groups.length === 1) return;

    const groups = [...presetDraft.groups];
    const ads = [...presetDraft.ads];

    groups.splice(index, 1);
    ads.splice(index, 1);

    setPresetDraft({ ...presetDraft, groups, ads });

    if (
      selectedStructure.type === "group" &&
      (selectedStructure.index ?? 0) === index
    ) {
      setSelectedStructure({ type: "company" });
    }
  };

  const savePreset = async () => {
    if (!userId || !presetDraft) return;
    setSaving(true);
    setError(null);
    try {
      const presetId =
        view.type === "presetEditor" && view.presetId ? view.presetId : undefined;
      const targetAction = presetDraft.company.targetAction;
      const allowed = allowedPlacementIdsForTarget(targetAction);
      // --- собрать/сохранить текстовые наборы из объявлений перед сохранением пресета
      if (presetDraft) {
        let nextTextSets = [...textSets];
        for (const ad of presetDraft.ads) {
          if (
            ad.textSetId &&
            (
              ad.newTextSetName.trim() ||
              ad.title ||
              ad.shortDescription ||
              ad.longDescription ||
              ad.advertiserInfo ||
              ad.logoId
            )
          ) {
            const exists = nextTextSets.some(s => s.id === ad.textSetId);
            // Находим существующий набор для сохранения полей text_swap/text_symbols
            const existingSet = nextTextSets.find(s => s.id === ad.textSetId);
            const newSet: TextSet = {
              id: ad.textSetId!,
              name: ad.newTextSetName.trim() || "(без названия)",
              title: ad.title ?? "",
              shortDescription: ad.shortDescription,
              longDescription: ad.longDescription,
              ...(ad.advertiserInfo ? { advertiserInfo: ad.advertiserInfo } : {}),
              ...(ad.button ? { button: ad.button } : {}),
              ...(ad.logoId ? { logoId: ad.logoId } : {}),
              // Сохраняем поля text_swap/text_symbols из существующего набора
              ...(existingSet?.short_text_swap ? { short_text_swap: existingSet.short_text_swap } : {}),
              ...(existingSet?.short_text_symbols ? { short_text_symbols: existingSet.short_text_symbols } : {}),
              ...(existingSet?.long_text_swap ? { long_text_swap: existingSet.long_text_swap } : {}),
              ...(existingSet?.long_text_symbols ? { long_text_symbols: existingSet.long_text_symbols } : {}),
            };
            nextTextSets = exists
              ? nextTextSets.map(s => s.id === ad.textSetId ? newSet : s)
              : [...nextTextSets, newSet];
          }
        }
        if (JSON.stringify(nextTextSets) !== JSON.stringify(textSets)) {
          await upsertTextSets(nextTextSets);
        }
      }
      const safePreset: Preset = {
        ...presetDraft,
        groups: presetDraft.groups.map(g => {
          const src = Array.isArray(g.placements) ? g.placements : [];
          const filtered = allowed ? src.filter(id => allowed.has(Number(id))) : src;
          return { ...g, placements: filtered };
        }),
        ads: presetDraft.ads.map(a => ({ ...a, logoId: a.logoId || (logo?.id ?? "") })),
      };

      const resp = await fetchSecured(`${API_BASE}/preset/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          presetId,
          preset: safePreset,
        }),
      });

      const json = await resp.json();
      if (!resp.ok) {
        throw new Error(json.detail || "Ошибка сохранения");
      }

      // обновляем список
      const pResp = await fetchSecured(
        `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
      );
      const pJson = await pResp.json();
      setPresets(pJson.presets || []);

      setView({ type: "home" });
      setPresetDraft(null);
    } catch (e: any) {
      console.error(e);
      showPopup(e.message || "Ошибка сохранения пресета");
    } finally {
      setSaving(false);
    }
  };

  const togglePresetActive = async (presetId: string) => {
    const cur = queueStatus[presetId] || "active";
    const next: "active"|"deactive" = (cur === "active" ? "deactive" : "active");
    // оптимистичное обновление
    setQueueStatus(prev => ({ ...prev, [presetId]: next }));
    try {
      await setQueueStatusApi(userId!, selectedCabinetId, presetId, next);
    } catch (e) {
      // откат при ошибке
      setQueueStatus(prev => ({ ...prev, [presetId]: cur }));
      showPopup("Не удалось изменить статус");
    }
  };

  const copyPreset = async (p: { preset_id: string; data: Preset }) => {
    if (!userId) return;
    try {
      // 1) глубокая нормализация
      const src = normalizePreset(p.data);

      // 2) жёсткое правило "один ролик" + строковые ID
      const isFast = !!src.fastPreset;
          
      const fixedAds = (src.ads || []).map(ad => {
        const vids = Array.isArray(ad.videoIds) ? ad.videoIds.map(String) : [];
        const imgs = Array.isArray(ad.imageIds) ? ad.imageIds.map(String) : [];
        const sets = Array.isArray(ad.creativeSetIds) ? ad.creativeSetIds.map(String) : [];
      
        return {
          ...ad,
          id: generateId(),
          videoIds: isFast ? vids : (vids.length ? [vids[0]] : []),
          imageIds: isFast ? imgs : (imgs.length ? [imgs[0]] : []),
          creativeSetIds: sets,
        };
      });

      const clone: Preset = {
        ...src,
        company: {
          ...(src.company || {}),
          presetName: `${src.company?.presetName || p.preset_id || "Без имени"} (копия)`,
        },
        groups: (src.groups || []).map(g => ({ ...g, id: String(g.id) })),
        ads: fixedAds,
      };

      // 3) сохраняем как новый пресет (без presetId)
      const resp = await fetchSecured(`${API_BASE}/preset/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          preset: clone,
        }),
      });
      const json = await resp.json();
      if (!resp.ok) throw new Error(json.detail || "Ошибка копирования");

      // 4) перечитать список
      const pResp = await fetchSecured(
        `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
      );
      const pJson = await pResp.json();
      setPresets(Array.isArray(pJson.presets) ? pJson.presets : []);
    } catch (e: any) {
      console.error(e);
      showPopup(e.message || "Не удалось скопировать пресет");
    }
  };

  const deletePreset = async (presetId: string) => {
    if (!userId) return;
    if (!(await askConfirm("Удалить этот пресет?"))) return;

    setSaving(true);
    setError(null);
    try {
      const resp = await fetchSecured(
        `${API_BASE}/preset/delete?user_id=${encodeURIComponent(
          userId
        )}&cabinet_id=${encodeURIComponent(
          selectedCabinetId || "all"
        )}&preset_id=${encodeURIComponent(presetId)}`,
        { method: "DELETE" }
      );
      if (!resp.ok) {
        throw new Error("Ошибка удаления пресета");
      }

      const pResp = await fetchSecured(
        `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
      );
      const pJson = await pResp.json();
      setPresets(pJson.presets || []);
    } catch (e: any) {
      console.error(e);
      showPopup(e.message || "Ошибка удаления пресета");
    } finally {
      setSaving(false);
    }
  };

  const parseHHMM = (s?: string) => {
    if (!s) return -1;
    const m = /^(\d{1,2}):(\d{2})$/.exec(s.trim());
    if (!m) return -1;
    const h = Math.min(23, Math.max(0, parseInt(m[1],10)));
    const min = Math.min(59, Math.max(0, parseInt(m[2],10)));
    return h*60+min;
  };

  const sortedPresets = useMemo(() => {
    const arr = [...presets];
    arr.sort((a, b) => {
      const an = (a?.data?.company?.presetName || a?.preset_id || "").toLowerCase();
      const bn = (b?.data?.company?.presetName || b?.preset_id || "").toLowerCase();

      const ac = Date.parse(a?.created_at || "") || 0;
      const bc = Date.parse(b?.created_at || "") || 0;

      const at = parseHHMM(a?.data?.company?.time || "");
      const bt = parseHHMM(b?.data?.company?.time || "");

      let cmp = 0;
      if (sortBy === "name") cmp = an.localeCompare(bn, "ru");
      if (sortBy === "created") cmp = ac - bc;    // свежие больше
      if (sortBy === "trigger") cmp = at - bt;    // раньше меньше

      return sortDir === "asc" ? cmp : -cmp;
    });
    return arr;
  }, [presets, sortBy, sortDir]);

  // ----------------- Creatives helpers -----------------

  const currentCreativeSet = useMemo(
    () => creativeSets.find((s) => s.id === selectedCreativeSetId) || null,
    [creativeSets, selectedCreativeSetId]
  );

  const upsertTextSets = async (sets: TextSet[]) => {
    await fetchSecured(`${API_BASE}/textsets/save`, {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({
        userId,
        cabinetId: selectedCabinetId,
        textsets: sets
      })
    });
    setTextSets(sets);
  };

  const savePixels = async (items: SitePixel[]) => {
    await fetchSecured(`${API_BASE}/pixels/save`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        userId,
        cabinetId: selectedCabinetId,
        pixels: items, // <-- сохраняем ОБЪЕКТЫ {pixel, domain}
      }),
    });
    setSitePixels(items);
  };

  const createCreativeSet = () => {
    const id = generateId();
    const newSet: CreativeSet = {
      id,
      name: `Набор ${creativeSets.length + 1}`,
      items: [],
    };
    const list = [...creativeSets, newSet];
    setCreativeSets(list);
    setSelectedCreativeSetId(id);
    saveCreatives(list);
  };

  const saveCreatives = async (list: CreativeSet[]) => {
    if (!userId || !selectedCabinetId) return;
    try {
      await fetchSecured(`${API_BASE}/creatives/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          creatives: list
        }),
      });
    } catch (e) {
      console.error(e);
    }
  };

  const renameCreativeSet = (id: string, name: string) => {
    setCreativeSets((prev) => {
      const next = prev.map((s) => (s.id === id ? { ...s, name } : s));
      creativeSetsRef.current = next;
      return next;
    });
    scheduleSaveCreatives();
  };

  const deleteCreativeSet = async (id: string) => {
    if (!(await askConfirm("Удалить набор креативов?"))) return;
    const list = creativeSets.filter((s) => s.id !== id);
    setCreativeSets(list);
    if (selectedCreativeSetId === id) {
      setSelectedCreativeSetId(list[0]?.id ?? null);
    }
    saveCreatives(list);
  };

  // creative
  const saveCreativesTimerRef = React.useRef<number | null>(null);
  const creativeSetsRef = React.useRef<CreativeSet[]>([]);
  React.useEffect(() => { creativeSetsRef.current = creativeSets; }, [creativeSets]);

  const scheduleSaveCreatives = React.useCallback(() => {
    if (saveCreativesTimerRef.current) window.clearTimeout(saveCreativesTimerRef.current);
    saveCreativesTimerRef.current = window.setTimeout(() => {
      saveCreatives(creativeSetsRef.current);
    }, 500);
  }, [saveCreatives]);

  const CREATIVE_CHUNK = 60;
  const [creativeLimit, setCreativeLimit] = React.useState(CREATIVE_CHUNK);
  const creativesSentinelRef = React.useRef<HTMLDivElement | null>(null);

  React.useEffect(() => {
    setCreativeLimit(CREATIVE_CHUNK);
  }, [selectedCreativeSetId]);

  React.useEffect(() => {
    if (!currentCreativeSet) return;
    const sentinel = creativesSentinelRef.current;
    if (!sentinel) return;

    const io = new IntersectionObserver(
      (entries) => {
        if (!entries[0]?.isIntersecting) return;
        setCreativeLimit((l) =>
          Math.min(l + CREATIVE_CHUNK, currentCreativeSet.items.length)
        );
      },
      { root: null, rootMargin: "400px", threshold: 0.01 }
    );

    io.observe(sentinel);
    return () => io.disconnect();
  }, [currentCreativeSet?.id, currentCreativeSet?.items.length]);

  // ---- Кроп картинок: очередь и управление модалкой ----
  const startCropForTask = (task: CropTask) => {
    if (cropPreviewUrl) {
      URL.revokeObjectURL(cropPreviewUrl);
    }
    setCurrentCropTask(task);
    setCropPreviewUrl(URL.createObjectURL(task.file));
    setCropFormatId("600x600");
    setCropRect({ x: 0, y: 0, width: 0, height: 0 });
    setCropModalOpen(true);
  };

  const enqueueImagesToCrop = (files: File[]) => {
    if (!currentCreativeSet) return;
    setCropQueue((prev) => {
      const tasks: CropTask[] = files.map((file) => ({
        file,
        setId: currentCreativeSet.id,
      }));
      const wasEmpty = prev.length === 0;
      const next = [...prev, ...tasks];
      if (wasEmpty && tasks.length > 0) {
        startCropForTask(tasks[0]);
      }
      return next;
    });
  };

  const advanceCropQueue = () => {
    setCropQueue((prev) => {
      const [, ...rest] = prev;
      if (cropPreviewUrl) {
        URL.revokeObjectURL(cropPreviewUrl);
      }
      if (rest.length === 0) {
        setCropModalOpen(false);
        setCurrentCropTask(null);
        setCropPreviewUrl(null);
      } else {
        startCropForTask(rest[0]);
      }
      return rest;
    });
  };

  const handleCropImageLoad = () => {
    const img = cropImgRef.current;
    if (!img) return;
    const fmt = IMAGE_FORMATS.find((f) => f.id === cropFormatId)!;
    const rect = calcInitialCropForImage(img, fmt);
    setCropRect(rect);

    // было: измерение по getBoundingClientRect() самого img
    // стало: точное измерение с учётом отступов
    recomputeImgBox();
  };

  const recomputeImgBox = React.useCallback(() => {
    const img = cropImgRef.current;
    const wrap = cropInnerRef.current;
    if (!img || !wrap) return;

    const ir = img.getBoundingClientRect();      // ВНЕШНИЙ бокс <img>
    const wr = wrap.getBoundingClientRect();

    const cw = ir.width;      // контейнер внутри crop-inner
    const ch = ir.height;

    const iw = img.naturalWidth || 1;
    const ih = img.naturalHeight || 1;

    const imgAspect = iw / ih;
    const boxAspect = cw / ch;

    // Рассчитываем ФАКТИЧЕСКИЕ размеры РИСУНКА внутри <img> при object-fit: contain
    let drawnW: number, drawnH: number, offX = 0, offY = 0;
    if (boxAspect > imgAspect) {
      // заполняем по высоте
      drawnH = ch;
      drawnW = ch * imgAspect;
      offX = (cw - drawnW) / 2; // letterbox слева/справа
    } else {
      // заполняем по ширине
      drawnW = cw;
      drawnH = cw / imgAspect;
      offY = (ch - drawnH) / 2; // letterbox сверху/снизу
    }

    // Переводим в координаты относительно crop-inner
    setImgBox({
      left:  (ir.left - wr.left) + offX,
      top:   (ir.top  - wr.top)  + offY,
      width: drawnW,
      height:drawnH,
    });

  }, []);

  const handleChangeFormat = (id: CropFormatId) => {
    setCropFormatId(id);
    const img = cropImgRef.current;
    if (!img) return;
    const fmt = IMAGE_FORMATS.find((f) => f.id === id)!;
    const rect = calcInitialCropForImage(img, fmt);
    setCropRect(rect);
    recomputeImgBox(); // ← ДОБАВИТЬ
  };

  React.useEffect(() => {
    if (!cropModalOpen) return;

    const img = cropImgRef.current;
    const ro = img ? new ResizeObserver(() => recomputeImgBox()) : null;
    if (img && ro) ro.observe(img);

    const onScroll = () => recomputeImgBox();
    // слушаем захватывающим образом все скроллы внутри модалки/страницы
    document.addEventListener("scroll", onScroll, true);
    window.addEventListener("resize", recomputeImgBox);

    // первый расчёт
    recomputeImgBox();

    return () => {
      document.removeEventListener("scroll", onScroll, true);
      window.removeEventListener("resize", recomputeImgBox);
      ro?.disconnect();
    };
  }, [cropModalOpen, recomputeImgBox]);

  const handleCropMouseMove = React.useCallback((e: MouseEvent) => {
    const st = cropDragState.current;
    const img = cropImgRef.current;
    if (!st || !st.active || !img) return;

    const dxPx = e.clientX - st.startX;
    const dyPx = e.clientY - st.startY;

    const bounds = img.getBoundingClientRect();
    const scaleX = img.naturalWidth / (bounds.width || 1);
    const scaleY = img.naturalHeight / (bounds.height || 1);

    const dxImg = dxPx * scaleX;
    const dyImg = dyPx * scaleY;

    const iw = img.naturalWidth;
    const ih = img.naturalHeight;

    setCropRect((prev) => {
      const width = prev.width;
      const height = prev.height;
      let x = st.startRect.x + dxImg;
      let y = st.startRect.y + dyImg;

      x = Math.max(0, Math.min(x, iw - width));
      y = Math.max(0, Math.min(y, ih - height));

      return { x, y, width, height };
    });
  }, []);

  const handleCropMouseUp = React.useCallback(() => {
    if (cropDragState.current) {
      cropDragState.current.active = false;
    }
    window.removeEventListener("mousemove", handleCropMouseMove);
    window.removeEventListener("mouseup", handleCropMouseUp);
  }, [handleCropMouseMove]);

  const handleCropMouseDown = (e: React.MouseEvent<HTMLDivElement>) => {
    e.preventDefault();
    const img = cropImgRef.current;
    if (!img) return;

    cropDragState.current = {
      active: true,
      startX: e.clientX,
      startY: e.clientY,
      startRect: { x: cropRect.x, y: cropRect.y },
    };

    window.addEventListener("mousemove", handleCropMouseMove);
    window.addEventListener("mouseup", handleCropMouseUp);
  };

  const handleCropCancel = () => {
    if (!currentCropTask) return;
    // уменьшить счётчик "загружаемых" — мы его увеличили при выборе файла
    setUploadingCount((prev) => Math.max(0, prev - 1));
    advanceCropQueue();
  };

  const handleCropConfirm = async () => {
    if (!currentCropTask || !userId) {
      advanceCropQueue();
      return;
    }
    const img = cropImgRef.current;
    if (!img) return;
    const fmt = IMAGE_FORMATS.find((f) => f.id === cropFormatId)!;

    try {
      const blob = await cropImageToBlob(img, cropRect, fmt.width, fmt.height);
      const fileName = currentCropTask.file.name || "image.jpg";
      const croppedFile = new File([blob], fileName, { type: "image/jpeg" });

      const formData = new FormData();
      formData.append("file", croppedFile);
      const curSet = creativeSets.find(s => s.id === currentCropTask.setId) || null;
      const resp = await fetchSecured(
        buildUploadUrl({
          userId,
          cabinetId: selectedCabinetId,
          setId: currentCropTask.setId,
          setName: curSet?.name || undefined,
        }),
        { method: "POST", body: formData }
      );
      const json = await resp.json();

      if (!resp.ok || !json.results) {
        console.error("UPLOAD ERROR:", json);
        showPopup("Ошибка загрузки файла");
      } else {
        let newItem: CreativeItem | null = null;

        // один кабинет
        if (json.results.length === 1) {
          const r = json.results[0];
          newItem = {
            id: String(r.vk_id),
            url: r.url,
            name: r.display_name || fileName,
            type: "image",
            uploaded: true,
            vkByCabinet: { [r.cabinet_id]: r.vk_id },
            ...(r.thumb_url ? { thumbUrl: r.thumb_url } : {}),
          };
        } else {
          // выбран "all" — много кабинетов
          const vkByCabinet: Record<string, any> = {};
          const urls: Record<string, string> = {};
          json.results.forEach((r: any) => {
            vkByCabinet[r.cabinet_id] = r.vk_id;
            urls[r.cabinet_id] = r.url;
          });
          const first = json.results[0] || {};
          const firstUrl: string = first.url || "";

          newItem = {
            id: generateId(),
            name: fileName,
            url: firstUrl,
            type: "image",
            uploaded: true,
            vkByCabinet,
            urls,
            ...(first.thumb_url ? { thumbUrl: first.thumb_url } : {}),
          };
        }

        if (newItem) {
          let updatedSets: CreativeSet[] = [];
          setCreativeSets((prev) => {
            updatedSets = prev.map((s) =>
              s.id === currentCropTask.setId
                ? { ...s, items: [...s.items, newItem!] }
                : s
            );
            return updatedSets;
          });
          if (updatedSets.length) {
            saveCreatives(updatedSets);
          }
        }
      }
    } catch (e) {
      console.error(e);
      showPopup("Ошибка обрезки/загрузки");
    } finally {
      setUploadingCount((prev) => Math.max(0, prev - 1));
      advanceCropQueue();
    }
  };

  const uploadCreativeFiles = async (files: FileList | null) => {
    if (!files || !currentCreativeSet) return;
    const all = Array.from(files);
    if (!all.length) return;

    // считаем все файлы как "в процессе"
    setUploadingCount((prev) => prev + all.length);

    const videoItems: CreativeItem[] = [];
    const imageFiles: File[] = [];

    for (const file of all) {
      if (file.type.startsWith("image")) {
        imageFiles.push(file);
        continue; // картинки пойдут в модалку кропа
      }

      try {
        const formData = new FormData();
        formData.append("file", file);
        if (!userId) return;

        const resp = await fetchSecured(
          buildUploadUrl({
            userId,
            cabinetId: selectedCabinetId,
            setId: currentCreativeSet.id,                // 👈 ключевая строка
            setName: currentCreativeSet.name,
          }),
          { method: "POST", body: formData }
        );
        const json = await resp.json();
        if (!resp.ok || !json.results) {
          console.error("UPLOAD ERROR:", json);
          showPopup("Ошибка загрузки файла");
          continue;
        }

        // один кабинет
        if (json.results.length === 1) {
          const r = json.results[0];

          videoItems.push({
            id: String(r.vk_id),
            url: r.url,
            name: r.display_name || file.name,
            type: "video",
            uploaded: true,
            vkByCabinet: { [r.cabinet_id]: r.vk_id },
            ...(r.thumb_url ? { thumbUrl: r.thumb_url } : {}),
          });
        } else {
          // выбран "all" — много кабинетов
          const vkByCabinet: Record<string, any> = {};
          const urls: Record<string, string> = {};

          json.results.forEach((r: any) => {
            vkByCabinet[r.cabinet_id] = r.vk_id;
            urls[r.cabinet_id] = r.url;
          });

          const first = json.results[0] || {};
          const firstUrl: string = first.url || "";

          videoItems.push({
            id: generateId(),
            name: file.name,
            url: firstUrl,
            type: "video",
            uploaded: true,
            vkByCabinet,
            urls,
            ...(first.thumb_url ? { thumbUrl: first.thumb_url } : {}),
          });
        }
      } finally {
        // за каждый видос уменьшаем счётчик
        setUploadingCount((prev) => Math.max(0, prev - 1));
      }
    }

    // добавляем загруженные видео в набор
    if (videoItems.length) {
      let updated: CreativeSet[] = [];
      setCreativeSets((prev) => {
        updated = prev.map((s) =>
          s.id === currentCreativeSet.id
            ? { ...s, items: [...s.items, ...videoItems] }
            : s
        );
        return updated;
      });
      if (updated.length) {
        saveCreatives(updated);
      }
    }

    // картинки — в очередь кропа (счётчик уменьшится при confirm/cancel)
    if (imageFiles.length) {
      enqueueImagesToCrop(imageFiles);
    }
  };

  const deleteCreativeItem = async (setId: string, itemId: string) => {
    const set = creativeSets.find(s => s.id === setId);
    const item = set?.items.find(it => it.id === itemId);
    if (!set || !item) return;
  
    try {
      await fetchSecured(`${API_BASE}/creative/delete`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          item: {
            type: item.type,
            url: item.url,
            urls: item.urls ?? null,
            thumbUrl: item.thumbUrl ?? null,
          }
        }),
      });
    } catch (e) {
      console.warn("creative/delete failed", e);
      // не блокируем UX: всё равно удалим из интерфейса
    }
  
    const list = creativeSets.map((s) =>
      s.id === setId
        ? { ...s, items: s.items.filter((it) => it.id !== itemId) }
        : s
    );
    setCreativeSets(list);
    saveCreatives(list);
  };

  // ----------------- Video picker (for ad) -----------------

  const openVideoPickerForAd = (adId: string) => {
    setVideoPicker({ open: true, adId });
  };

  const closeVideoPicker = () => {
    setVideoPicker({ open: false, adId: null });
  };

  const toggleMediaForAd = (adId: string, item: CreativeItem) => {
    if (!presetDraft) return;
    const ads = presetDraft.ads.map((ad) => {
      if (ad.id !== adId) return ad;
    
      const selectedIn =
        item.type === "image"
          ? (ad.imageIds || [])
          : (ad.videoIds || []);
    
      const isSelected = selectedIn.some(vid =>
        videoIdMatchesItem(vid, item, selectedCabinetId)
      );
    
      if (item.type === "image") {
        const next = isSelected
          ? (ad.imageIds || []).filter(vid => !videoIdMatchesItem(vid, item, selectedCabinetId))
          : [...(ad.imageIds || []), String(item.id)];
        return { ...ad, imageIds: next };
      } else {
        const next = isSelected
          ? (ad.videoIds || []).filter(vid => !videoIdMatchesItem(vid, item, selectedCabinetId))
          : [...(ad.videoIds || []), String(item.id)];
        return { ...ad, videoIds: next };
      }
    });
    setPresetDraft({ ...presetDraft, ads });
  };

  const toggleCreativeSetForAd = (adId: string, set: CreativeSet) => {
    if (!presetDraft) return;
    const ads = presetDraft.ads.map((ad) => {
      if (ad.id !== adId) return ad;
      const already = ad.creativeSetIds.includes(set.id);
      return {
        ...ad,
        creativeSetIds: already
          ? ad.creativeSetIds.filter((id) => id !== set.id)
          : [...ad.creativeSetIds, set.id],
      };
    });
    setPresetDraft({ ...presetDraft, ads });
  };

  const getAdById = (adId: string) =>
    presetDraft?.ads.find((a) => a.id === adId) || null;

  // ----------------- Render helpers -----------------

  const renderHeader = () => (
    <header className="app-header glass">
      <div className="header-left">
        <button className="icon-button" onClick={toggleTheme} title="Переключить тему">
          {theme === "light" ? "☀️" : "🌙"}
        </button>
      </div>

      <div className="header-center">
        <h1 className="app-title">Auto ADS</h1>
      </div>

      <div className="header-right">
        <div className="header-cabinet-row">
          {/* Кнопка выбора sub1 */}
          <button 
            className="icon-button"
            onClick={() => {
              setSub1Selected(userSub1);
              setSub1Search("");
              setSub1ModalOpen(true);
            }}
            title="Выбрать sub1"
            style={{ marginRight: 8, position: "relative", marginBottom: 7, borderRadius: 10 }}
          >
            <IconMoney className="header-icon" size={25} />

          </button>
          
          <CabinetSelect
            cabinets={cabinets}
            value={selectedCabinetId}
            disabled={cabinets.length === 0}
            onChange={(id) => {
              setSelectedCabinetId(id);

              fetchSecured(`${API_BASE}/settings/save`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  userId,
                  settings: { selected_cabinet_id: id },
                }),
              }).catch(console.error);
            }}
          />
        </div>
      </div>
    </header>
  );

  //const CAN_COMPANIES_USER_ID = "1342381428";
  const canAccessCompanies = true;

  const renderSidebar = () => (
    <>
      <div
        className="sidebar-hover-zone"
        style={{ pointerEvents: sidebarOpen ? "none" : "auto" }}
        onMouseEnter={openSidebar}
        aria-hidden="true"
      />

      <aside
        className="sidebar glass"
        onMouseEnter={openSidebar}
        onMouseLeave={scheduleCloseSidebar}
      >
        <div className="sidebar-tabs">
          {/* Создание кампаний с подвкладками */}
          <div className="sidebar-tab-group">
            <button
              className={`sidebar-tab ${activeTab === "campaigns" ? "active" : ""}`}
              onClick={() => {
                setActiveTab("campaigns");
                setView({ type: "home" });
              }}
            >
              <IconCampaign className="tab-icon" />
              <span className="tab-label">Создание кампаний</span>
            </button>

            {/* Подвкладки - показываем только когда campaigns активна */}
            {activeTab === "campaigns" && (
              <div
                className={`sidebar-subtabs ${
                  activeTab === "campaigns" ? "open" : ""
                }`}
              >
                <svg
                  className="sidebar-tree"
                  width="32"
                  viewBox="0 0 32 100"
                  preserveAspectRatio="none"
                  aria-hidden
                >
                  {/* вертикальная линия */}
                  <line
                    x1="16"
                    y1="0"
                    x2="16"
                    y2="50"
                    stroke="currentColor"
                    strokeWidth="3"
                    strokeLinecap="round"
                  />

                  {/* ветка к первой кнопке */}
                  <line
                    x1="18"
                    y1="16"
                    x2="28"
                    y2="16"
                    stroke="currentColor"
                    strokeWidth="3"
                    strokeLinecap="round"
                  />

                  {/* ветка ко второй кнопке */}
                  <line
                    x1="18"
                    y1="50"
                    x2="28"
                    y2="50"
                    stroke="currentColor"
                    strokeWidth="3"
                    strokeLinecap="round"
                  />
                </svg>
                <button
                  className={`sidebar-subtab ${campaignsSubTab === "presets" ? "active" : ""}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    setCampaignsSubTab("presets");
                    setView({ type: "home" });
                  }}
                >
                  Создание пресетов
                </button>
                <button
                  className={`sidebar-subtab ${
                    campaignsSubTab === "companies" ? "active" : ""
                  } ${!canAccessCompanies ? "disabled" : ""}`}
                  disabled={!canAccessCompanies}
                  onClick={(e) => {
                    e.stopPropagation();

                    if (!canAccessCompanies) {
                      return;
                    }

                    setCampaignsSubTab("companies");
                    setView({ type: "home" });
                  }}
                >
                  Компании
                </button>
              </div>
            )}
          </div>

          <button
            className={`sidebar-tab ${activeTab === "creatives" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("creatives");
              setView({ type: "home" });
            }}
          >
            <IconCreatives className="tab-icon" />
            <span className="tab-label">Креативы</span>
          </button>

          <button
            className={`sidebar-tab ${activeTab === "logo" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("logo");
              setView({ type: "home" });
            }}
          >
            <IconLogo className="tab-icon" />
            <span className="tab-label">Логотип</span>
          </button>

          <button
            className={`sidebar-tab ${activeTab === "audiences" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("audiences");
              setView({ type: "home" });
            }}
          >
            <IconAudience3 className="tab-icon" />
            <span className="tab-label">Аудитории</span>
          </button>
          <button
            className={`sidebar-tab ${activeTab === "textsets" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("textsets");
              setView({ type: "home" });
            }}
          >
            <IconText className="tab-icon" />
            <span className="tab-label">Тексты</span>
          </button>
          <button
            className={`sidebar-tab ${activeTab === "history" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("history");
              setView({ type: "home" });
            }}
          >
            <IconHistory className="tab-icon" />
            <span className="tab-label">История</span>
          </button>

          <button
            className={`sidebar-tab ${activeTab === "settings" ? "active" : ""}`}
            onClick={() => {
              setActiveTab("settings");
              setView({ type: "home" });
            }}
          >
            <IconSettings className="tab-icon" />
            <span className="tab-label">Настройки</span>
          </button>

          {/* Разное с подвкладками Триггер и Триггер пресета - только для user_id 1342381428 */}
          {userId === "1342381428" && (
            <button
              className={`sidebar-tab ${activeTab === "misc" ? "active" : ""}`}
              onClick={() => {
                setActiveTab("misc");
                setView({ type: "home" });
              }}
            >
              <IconMisc className="tab-icon" />
              <span className="tab-label">Разное</span>
            </button>
          )}
        </div>
      </aside>
    </>
  );

  const renderSettingsPage = () => {
    return (
      <div className="content-section glass">
        <div className="section-header">
          <h2>Настройки</h2>
        </div>

        {/* Подвкладки */}
        <div className="settings-tabs">
          <button
            className={`settings-tab ${settingsTab === "general" ? "active" : ""}`}
            onClick={() => setSettingsTab("general")}
          >
            Основные
          </button>
          <button
            className={`settings-tab ${settingsTab === "notifications" ? "active" : ""}`}
            onClick={() => setSettingsTab("notifications")}
          >
            Уведомления
          </button>
          <button
            className={`settings-tab ${settingsTab === "autoReupload" ? "active" : ""}`}
            onClick={() => setSettingsTab("autoReupload")}
          >
            Авто-перезалив
          </button>
        </div>

        {/* Контент вкладок */}
        {settingsTab === "general" && (
          <div className="settings-content">
            <div className="hint">Пока нету</div>
          </div>
        )}

        {settingsTab === "notifications" && (
          <div className="settings-content">
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={notifyOnError}
                onChange={(e) => saveNotificationSettings({ notifyOnError: e.target.checked })}
              />
              <span>Уведомлять при ошибке создания компании</span>
            </label>
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={notifyOnCreate}
                onChange={(e) => saveNotificationSettings({ notifyOnCreate: e.target.checked })}
              />
              <span>Уведомлять при создании</span>
            </label>
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={notifyOnReupload}
                onChange={(e) => saveNotificationSettings({ notifyOnReupload: e.target.checked })}
              />
              <span>Уведомлять при перезаливе</span>
            </label>
          </div>
        )}

        {settingsTab === "autoReupload" && (
          <div className="settings-content">
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={autoReuploadEnabled}
                onChange={(e) => saveAutoReuploadSettings({ enabled: e.target.checked })}
              />
              <span>Включить авто-перезалив</span>
            </label>
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={deleteRejected}
                onChange={(e) => saveAutoReuploadSettings({ deleteRejected: e.target.checked })}
              />
              <span>Удалять отклоненные компании/группы</span>
            </label>
            <label className="settings-checkbox">
              <input
                type="checkbox"
                checked={skipModerationFail}
                onChange={(e) => saveAutoReuploadSettings({ skipModerationFail: e.target.checked })}
              />
              <span>Не заливать компании/группы которые не пройдут модерацию</span>
            </label>
            
            <div className="settings-time-range">
              <span className="settings-time-label">Пересоздавать в диапазоне</span>
              <input
                type="time"
                className="settings-time-input"
                value={reuploadTimeStart}
                onChange={(e) => saveAutoReuploadSettings({ timeStart: e.target.value })}
              />
              <span className="settings-time-separator">—</span>
              <input
                type="time"
                className="settings-time-input"
                value={reuploadTimeEnd}
                onChange={(e) => saveAutoReuploadSettings({ timeEnd: e.target.value })}
              />
              <span 
                className="settings-tooltip-icon"
                title="Перезаливы вне этого диапазона будут перенесены на следующий день"
              >
                <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg" style={{marginLeft:410,marginTop:-26}}>
                  <circle cx="8" cy="8" r="7" stroke="currentColor" strokeWidth="1.5"/>
                  <path d="M8 7V11" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                  <circle cx="8" cy="5" r="0.75" fill="currentColor"/>
                </svg>
              </span>
            </div>
          </div>
        )}
      </div>
    );
  };

  const renderBackBar = () => {
    if (view.type === "home") return null;
    return (
      <div className="back-bar">
        <button
          className="icon-button"
          onClick={() => {
            setView({ type: "home" });
            setPresetDraft(null);
          }}
        >
          <IconArrowLeft className="icon" />
        </button>
        <span className="back-bar-title">
          {view.type === "presetEditor" && "Создание пресета"}
          {view.type === "creativeSetEditor" && "Набор креативов"}
        </span>
      </div>
    );
  };

  // === RENDER COMPANIES TABLE ===
  const renderCompaniesTable = () => {
    const visibleColumns = companiesColumns.filter(c => c.visible);
    const allSelected = sortedVkCompanies.length > 0 && selectedCompanyIds.size === sortedVkCompanies.length;
    const someSelected = selectedCompanyIds.size > 0 && selectedCompanyIds.size < sortedVkCompanies.length;

    return (
      <div className="companies-table-wrapper">
        {/* Toolbar */}
        <div className="companies-toolbar">
          <div className="companies-toolbar-left">
            <button className="primary-button companies-create-btn">
              <IconPlus className="icon" />
              <span>Создать</span>
            </button>
            
            <div className="companies-actions-dropdown">
              <button className="outline-button" style={{borderRadius:7,padding:"6px 16px"}}>
                Действия
                <span style={{ marginLeft: 4, fontSize: 10 }}>▼</span>
              </button>
            </div>
            
            <button 
              className="icon-button companies-refresh-btn" 
              onClick={() => fetchVkCompanies(true)}
              title="Обновить"
              disabled={vkCompaniesLoading}
            >
              <IconRefresh className={`icon ${vkCompaniesLoading ? "spinning" : ""}`} />
            </button>
          </div>
          
          <div className="companies-toolbar-right" style={{ position: "relative" }}>
            <button 
              className="icon-button sq-dark-button" 
              style={{borderRadius:7}} 
              title="Настройки таблицы"
              onClick={() => setCompaniesColumnsMenuOpen(!companiesColumnsMenuOpen)}
            >
              <IconSettings className="icon" />
            </button>
            
            {companiesColumnsMenuOpen && 
              createPortal(
                <div 
                  className="columns-menu glass"
                  style={{
                    position: "absolute",
                    top: 190,
                    right: 100,
                    marginTop: 8,
                    padding: 12,
                    borderRadius: 12,
                    minWidth: 200,
                    zIndex: 100,
                    display: "flex",
                    flexDirection: "column",
                    gap: 8,
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>Столбцы</div>
                  {companiesColumns.map(col => (
                    <label 
                      key={col.id} 
                      style={{ 
                        display: "flex", 
                        alignItems: "center", 
                        gap: 8, 
                        cursor: "pointer",
                        fontSize: 13,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={col.visible}
                        onChange={() => {
                          setCompaniesColumns(prev => 
                            prev.map(c => c.id === col.id ? { ...c, visible: !c.visible } : c)
                          );
                        }}
                      />
                      {col.label}
                    </label>
                  ))}
                </div>,
              document.body
            )}
          </div>
        </div>

        {/* Table container with loading overlay */}
        <div className={`companies-table-container ${vkCompaniesLoading ? "loading" : ""}`}>
          {vkCompaniesLoading && (
            <div className="companies-loading-overlay">
              <div className="loader" />
            </div>
          )}
          
          <div className="companies-table-scroll">
            <table className="companies-table">
              <thead>
                <tr>
                  <th className="col-checkbox">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      ref={(el) => { if (el) el.indeterminate = someSelected; }}
                      onChange={toggleSelectAllCompanies}
                    />
                  </th>
                  <th className="col-toggle"></th>
                  {visibleColumns.map(col => (
                    <th
                      key={col.id}
                      style={{ width: col.width, minWidth: col.width }}
                      draggable
                      onDragStart={(e) => handleColumnDragStart(e, col.id)}
                      onDragOver={handleColumnDragOver}
                      onDrop={(e) => handleColumnDrop(e, col.id)}
                      onClick={() => handleSortClick(col)}
                      className={`
                        ${col.sortable ? "sortable" : ""}
                        ${companiesSorting.field === col.sortField ? "sorted" : ""}
                        ${draggedColumnId === col.id ? "dragging" : ""}
                      `}
                    >
                      <span className="col-label">{col.label}</span>
                      {col.sortable && companiesSorting.field === col.sortField && (
                        <span className="sort-icon">
                          {companiesSorting.dir === "asc" ? "↑" : "↓"}
                        </span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {sortedVkCompanies.map(company => {
                  const stats = vkCompaniesStats[company.id];
                  const hasActiveAds = companyHasActiveAds(company.id);
                  const statusInfo = company.status === "active" && !hasActiveAds 
                    ? { text: "Остановлена", className: "status-stopped" }
                    : formatVkStatus(company.status);
                  const isSelected = selectedCompanyIds.has(company.id);
                  const isToggling = companyTogglingIds.has(company.id);
                  const isActive = company.status === "active";

                  return (
                    <tr key={company.id} className={isSelected ? "selected" : ""}>
                      <td className="col-checkbox">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleCompanySelect(company.id)}
                        />
                      </td>
                      <td className="col-toggle">
                        <div
                          role="switch"
                          aria-checked={isActive}
                          className={`toggle-mini ${isActive ? "on" : ""} ${isToggling ? "toggling" : ""}`}
                          onClick={() => !isToggling && toggleCompanyStatus(company.id, company.status)}
                        >
                          <div className="toggle-knob" />
                        </div>
                      </td>
                      {visibleColumns.map(col => {
                        let content: React.ReactNode = "";

                        switch (col.id) {
                          case "name":
                            content = (
                              <div className="company-name-cell">
                                <span className="company-name-text" title={company.name}>
                                  {company.name}
                                </span>
                                <div className="company-name-actions">
                                  <button className="icon-button mini" title="Дублировать">
                                    <IconDuplicate className="icon" />
                                  </button>
                                  <button className="icon-button mini" title="Ещё">
                                    <IconMoreHorizontal className="icon" />
                                  </button>
                                </div>
                              </div>
                            );
                            break;
                          case "status":
                            content = (
                              <span className={`company-status ${statusInfo.className}`}>
                                <span className="status-dot" />
                                {statusInfo.text}
                              </span>
                            );
                            break;
                          case "id":
                            content = company.id;
                            break;
                          case "objective":
                            content = formatVkObjective(company.objective);
                            break;
                          case "goals":
                            content = formatNumber(stats?.base?.goals);
                            break;
                          case "cpa":
                            content = formatMoneyInt(stats?.base?.cpa);
                            break;
                          case "spent":
                            content = formatMoneyInt(stats?.base?.spent);
                            break;
                          case "clicks":
                            content = formatNumber(stats?.base?.clicks);
                            break;
                          case "cpc":
                            content = formatMoney(stats?.base?.cpc);
                            break;
                          case "shows":
                            content = formatNumber(stats?.base?.shows);
                            break;
                          case "created":
                            content = formatVkCreated(company.created);
                            break;
                          case "revenue":
                            content = formatNumber(getCompanyRevenue(company.id));
                            break;
                          case "profit": {
                            const profit = getCompanyProfit(company.id);
                            content = (
                              <span style={{ 
                                color: profit > 0 ? "#4caf50" : profit < 0 ? "#f44336" : "#888" 
                              }}>
                                {formatNumber(profit)}
                              </span>
                            );
                            break;
                          }
                        }

                        return (
                          <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                            {content}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className="totals-row">
                  <td className="col-checkbox"></td>
                  <td className="col-toggle"></td>
                  {visibleColumns.map(col => {
                    let content: React.ReactNode = "";

                    switch (col.id) {
                      case "name":
                        content = <strong>Итого: {vkCompaniesTotal} кампаний</strong>;
                        break;
                      case "goals":
                        content = <strong>{formatNumber(companiesTotals.goals)}</strong>;
                        break;
                      case "spent":
                        content = <strong>{formatMoneyInt(companiesTotals.spent)}</strong>;
                        break;
                      case "clicks":
                        content = <strong>{formatNumber(companiesTotals.clicks)}</strong>;
                        break;
                      case "cpc":
                        content = <strong>{formatMoney(companiesTotals.cpc)}</strong>;
                        break;
                      case "cpa":
                        content = <strong>{formatMoneyInt(companiesTotals.cpa)}</strong>;
                        break;
                      case "shows":
                        content = <strong>{formatNumber(companiesTotals.shows)}</strong>;
                        break;
                      case "revenue": {
                        const totalRevenue = sortedVkCompanies.reduce((sum, c) => sum + getCompanyRevenue(c.id), 0);
                        content = <strong>{formatNumber(totalRevenue)}</strong>;
                        break;
                      }
                      case "profit": {
                        const totalProfit = sortedVkCompanies.reduce((sum, c) => sum + getCompanyProfit(c.id), 0);
                        content = (
                          <strong style={{ 
                            color: totalProfit > 0 ? "#4caf50" : totalProfit < 0 ? "#f44336" : "#888" 
                          }}>
                            {formatNumber(totalProfit)}
                          </strong>
                        );
                        break;
                      }
                    }

                    return (
                      <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                        {content}
                      </td>
                    );
                  })}
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      </div>
    );
  };

  // === RENDER GROUPS TABLE ===
  const renderGroupsTable = () => {
    const groups = filteredGroups;
    const visibleColumns = groupsColumns.filter(c => c.visible);
    const allSelected = groups.length > 0 && selectedGroupIds.size === groups.length;
    const someSelected = selectedGroupIds.size > 0 && selectedGroupIds.size < groups.length;

    const companyNames: Record<number, string> = {};
    vkCompanies.forEach(c => { companyNames[c.id] = c.name; });

    return (
      <div className="companies-table-wrapper">
        <div className="companies-toolbar">
          <div className="companies-toolbar-left">
            <button className="primary-button companies-create-btn">
              <IconPlus className="icon" />
              <span>Создать</span>
            </button>
            
            <button 
              className="icon-button companies-refresh-btn" 
              onClick={() => fetchVkGroups(true)}
              title="Обновить"
              disabled={vkGroupsLoading}
            >
              <IconRefresh className={`icon ${vkGroupsLoading ? "spinning" : ""}`} />
            </button>
          </div>
          
          <div className="companies-toolbar-right" style={{ position: "relative" }}>
            <button 
              className="icon-button sq-dark-button" 
              style={{borderRadius:7}} 
              title="Настройки таблицы"
              onClick={() => setGroupsColumnsMenuOpen(!groupsColumnsMenuOpen)}
            >
              <IconSettings className="icon" />
            </button>
            
            {groupsColumnsMenuOpen && 
              createPortal(
                <div 
                  className="columns-menu glass"
                  style={{
                    position: "absolute",
                    top: 190,
                    right: 100,
                    marginTop: 8,
                    padding: 12,
                    borderRadius: 12,
                    minWidth: 200,
                    zIndex: 100,
                    display: "flex",
                    flexDirection: "column",
                    gap: 8,
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>Столбцы</div>
                  {groupsColumns.map(col => (
                    <label 
                      key={col.id} 
                      style={{ 
                        display: "flex", 
                        alignItems: "center", 
                        gap: 8, 
                        cursor: "pointer",
                        fontSize: 13,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={col.visible}
                        onChange={() => {
                          setGroupsColumns(prev => 
                            prev.map(c => c.id === col.id ? { ...c, visible: !c.visible } : c)
                          );
                        }}
                      />
                      {col.label}
                    </label>
                  ))}
                </div>,
              document.body
            )}
          </div>
        </div>

        <div className={`companies-table-container ${vkGroupsLoading ? "loading" : ""}`}>
          {vkGroupsLoading && (
            <div className="companies-loading-overlay">
              <div className="loader" />
            </div>
          )}
          
          <div className="companies-table-scroll">
            <table className="companies-table">
              <thead>
                <tr>
                  <th className="col-checkbox">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      ref={(el) => { if (el) el.indeterminate = someSelected; }}
                      onChange={toggleSelectAllGroups}
                    />
                  </th>
                  <th className="col-toggle"></th>
                  {visibleColumns.map(col => (
                    <th
                      key={col.id}
                      style={{ width: col.width, minWidth: col.width }}
                      draggable
                      onDragStart={(e) => handleGroupColumnDragStart(e, col.id)}
                      onDragOver={handleGroupColumnDragOver}
                      onDrop={(e) => handleGroupColumnDrop(e, col.id)}
                      onClick={() => handleGroupSortClick(col)}
                      className={`
                        ${col.sortable ? "sortable" : ""}
                        ${groupsSorting.field === col.sortField ? "sorted" : ""}
                        ${draggedGroupColumnId === col.id ? "dragging" : ""}
                      `}
                    >
                      <span className="col-label">{col.label}</span>
                      {col.sortable && groupsSorting.field === col.sortField && (
                        <span className="sort-icon">
                          {groupsSorting.dir === "asc" ? "↑" : "↓"}
                        </span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {groups.map((group, idx) => {
                  const stats = vkGroupsStats[group.id];
                  const isSelected = selectedGroupIds.has(group.id);
                  const isToggling = groupTogglingIds.has(group.id);
                  const isActive = group.status === "active";
                  const companyName = companyNames[group.ad_plan_id] || "—";

                  return (
                    <tr 
                      key={group.id} 
                      className={`${isSelected ? "selected" : ""} ${idx % 2 === 0 ? "company-even" : "company-odd"}`}
                    >
                      <td className="col-checkbox">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleGroupSelect(group.id)}
                        />
                      </td>
                      <td className="col-toggle">
                        <div
                          role="switch"
                          aria-checked={isActive}
                          className={`toggle-mini ${isActive ? "on" : ""} ${isToggling ? "toggling" : ""}`}
                          onClick={() => !isToggling && toggleGroupStatus(group.id, group.status)}
                        >
                          <div className="toggle-knob" />
                        </div>
                      </td>
                      {visibleColumns.map(col => {
                        let content: React.ReactNode = "";

                        switch (col.id) {
                          case "companyName":
                            content = (
                              <span className="nested-company-name" title={companyName}>
                                {companyName}
                              </span>
                            );
                            break;
                          case "name":
                            content = (
                              <span className="company-name-text" title={group.name}>
                                {group.name}
                              </span>
                            );
                            break;
                          case "status": {
                            const modStatus = getGroupModerationStatus(group.id);
                            const statusInfo = getEntityStatusInfo(group.status, modStatus);
                            content = (
                              <span className={`company-status ${statusInfo.className}`}>
                                <span className="status-dot" />
                                {statusInfo.text}
                              </span>
                            );
                            break;
                          }
                          case "budget":
                            content = formatBudget(group.budget_limit_day);
                            break;
                          case "goals":
                            content = formatNumber(stats?.base?.goals);
                            break;
                          case "cpa":
                            content = formatMoneyInt(stats?.base?.cpa);
                            break;
                          case "spent":
                            content = formatMoneyInt(stats?.base?.spent);
                            break;
                          case "clicks":
                            content = formatNumber(stats?.base?.clicks);
                            break;
                          case "shows":
                            content = formatNumber(stats?.base?.shows);
                            break;
                          case "created":
                            content = formatVkCreated(group.created);
                            break;
                          case "groupId":
                            content = group.id;
                            break;
                          case "revenue":
                            content = formatNumber(getGroupRevenue(group.id));
                            break;
                          case "profit": {
                            const profit = getGroupProfit(group.id);
                            content = (
                              <span style={{ 
                                color: profit > 0 ? "#4caf50" : profit < 0 ? "#f44336" : "#888" 
                              }}>
                                {formatNumber(profit)}
                              </span>
                            );
                            break;
                          }
                        }

                        return (
                          <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                            {content}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className="totals-row">
                  <td className="col-checkbox"></td>
                  <td className="col-toggle"></td>
                  {visibleColumns.map(col => {
                    let content: React.ReactNode = "";

                    switch (col.id) {
                      case "companyName":
                        content = <strong>Итого: {filteredGroups.length} групп</strong>;
                        break;
                      case "budget":
                        content = <strong>{formatBudget(groupsTotals.budget)}</strong>;
                        break;
                      case "goals":
                        content = <strong>{formatNumber(groupsTotals.goals)}</strong>;
                        break;
                      case "cpa":
                        content = <strong>{formatMoneyInt(groupsTotals.cpa)}</strong>;
                        break;
                      case "spent":
                        content = <strong>{formatMoneyInt(groupsTotals.spent)}</strong>;
                        break;
                      case "clicks":
                        content = <strong>{formatNumber(groupsTotals.clicks)}</strong>;
                        break;
                      case "shows":
                        content = <strong>{formatNumber(groupsTotals.shows)}</strong>;
                        break;
                      case "revenue": {
                        const totalRevenue = filteredGroups.reduce((sum, g) => sum + getGroupRevenue(g.id), 0);
                        content = <strong>{formatNumber(totalRevenue)}</strong>;
                        break;
                      }
                      case "profit": {
                        const totalProfit = filteredGroups.reduce((sum, g) => sum + getGroupProfit(g.id), 0);
                        content = (
                          <strong style={{ 
                            color: totalProfit > 0 ? "#4caf50" : totalProfit < 0 ? "#f44336" : "#888" 
                          }}>
                            {formatNumber(totalProfit)}
                          </strong>
                        );
                        break;
                      }
                    }

                    return (
                      <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                        {content}
                      </td>
                    );
                  })}
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      </div>
    );
  };

  // === RENDER ADS TABLE ===
  const renderAdsTable = () => {
    const ads = filteredAds;
    const visibleColumns = adsColumns.filter(c => c.visible);
    const allSelected = ads.length > 0 && selectedAdIds.size === ads.length;
    const someSelected = selectedAdIds.size > 0 && selectedAdIds.size < ads.length;

    const companyNames: Record<number, string> = {};
    const groupNames: Record<number, string> = {};
    const groupToCompany: Record<number, number> = {};
    
    vkCompanies.forEach(c => { companyNames[c.id] = c.name; });
    vkGroups.forEach(g => { 
      groupNames[g.id] = g.name;
      groupToCompany[g.id] = g.ad_plan_id;
    });

    return (
      <div className="companies-table-wrapper">
        <div className="companies-toolbar">
          <div className="companies-toolbar-left">
            <button className="primary-button companies-create-btn">
              <IconPlus className="icon" />
              <span>Создать</span>
            </button>
            
            <button 
              className="icon-button companies-refresh-btn" 
              onClick={() => fetchVkAds(true)}
              title="Обновить"
              disabled={vkAdsLoading}
            >
              <IconRefresh className={`icon ${vkAdsLoading ? "spinning" : ""}`} />
            </button>
          </div>
          
          <div className="companies-toolbar-right" style={{ position: "relative" }}>
            <button 
              className="icon-button sq-dark-button" 
              style={{borderRadius:7}} 
              title="Настройки таблицы"
              onClick={() => setAdsColumnsMenuOpen(!adsColumnsMenuOpen)}
            >
              <IconSettings className="icon" />
            </button>
            
            {adsColumnsMenuOpen && 
              createPortal(
                <div 
                  className="columns-menu glass"
                  style={{
                    position: "absolute",
                    top: 190,
                    right: 100,
                    marginTop: 8,
                    padding: 12,
                    borderRadius: 12,
                    minWidth: 200,
                    zIndex: 100,
                    display: "flex",
                    flexDirection: "column",
                    gap: 8,
                  }}
                >
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>Столбцы</div>
                  {adsColumns.map(col => (
                    <label 
                      key={col.id} 
                      style={{ 
                        display: "flex", 
                        alignItems: "center", 
                        gap: 8, 
                        cursor: "pointer",
                        fontSize: 13,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={col.visible}
                        onChange={() => {
                          setAdsColumns(prev => 
                            prev.map(c => c.id === col.id ? { ...c, visible: !c.visible } : c)
                          );
                        }}
                      />
                      {col.label}
                    </label>
                  ))}
                </div>,
              document.body
            )}
          </div>
        </div>

        <div className={`companies-table-container ${vkAdsLoading ? "loading" : ""}`}>
          {vkAdsLoading && (
            <div className="companies-loading-overlay">
              <div className="loader" />
            </div>
          )}
          
          <div className="companies-table-scroll">
            <table className="companies-table">
              <thead>
                <tr>
                  <th className="col-checkbox">
                    <input
                      type="checkbox"
                      checked={allSelected}
                      ref={(el) => { if (el) el.indeterminate = someSelected; }}
                      onChange={toggleSelectAllAds}
                    />
                  </th>
                  <th className="col-toggle"></th>
                  {visibleColumns.map(col => (
                    <th
                      key={col.id}
                      style={{ width: col.width, minWidth: col.width }}
                      draggable
                      onDragStart={(e) => handleAdColumnDragStart(e, col.id)}
                      onDragOver={handleAdColumnDragOver}
                      onDrop={(e) => handleAdColumnDrop(e, col.id)}
                      onClick={() => handleAdSortClick(col)}
                      className={`
                        ${col.sortable ? "sortable" : ""}
                        ${adsSorting.field === col.sortField ? "sorted" : ""}
                        ${draggedAdColumnId === col.id ? "dragging" : ""}
                      `}
                    >
                      <span className="col-label">{col.label}</span>
                      {col.sortable && adsSorting.field === col.sortField && (
                        <span className="sort-icon">
                          {adsSorting.dir === "asc" ? "↑" : "↓"}
                        </span>
                      )}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {ads.map((ad, idx) => {
                  const stats = vkAdsStats[ad.id];
                  const isSelected = selectedAdIds.has(ad.id);
                  const isToggling = adTogglingIds.has(ad.id);
                  const isActive = ad.status === "active";
                  const companyId = groupToCompany[ad.ad_group_id] || 0;
                  const companyName = companyNames[companyId] || "—";
                  const groupName = groupNames[ad.ad_group_id] || "—";

                  return (
                    <tr 
                      key={ad.id} 
                      className={`${isSelected ? "selected" : ""} ${idx % 2 === 0 ? "company-even" : "company-odd"}`}
                    >
                      <td className="col-checkbox">
                        <input
                          type="checkbox"
                          checked={isSelected}
                          onChange={() => toggleAdSelect(ad.id)}
                        />
                      </td>
                      <td className="col-toggle">
                        <div
                          role="switch"
                          aria-checked={isActive}
                          className={`toggle-mini ${isActive ? "on" : ""} ${isToggling ? "toggling" : ""}`}
                          onClick={() => !isToggling && toggleAdStatus(ad.id, ad.status)}
                        >
                          <div className="toggle-knob" />
                        </div>
                      </td>
                      {visibleColumns.map(col => {
                        let content: React.ReactNode = "";

                        switch (col.id) {
                          case "companyName":
                            content = (
                              <span className="nested-company-name" title={companyName}>
                                {companyName}
                              </span>
                            );
                            break;
                          case "groupName":
                            content = (
                              <span className="nested-group-name" title={groupName}>
                                {groupName}
                              </span>
                            );
                            break;
                          case "name":
                            content = (
                              <span className="company-name-text" title={ad.name}>
                                {ad.name}
                              </span>
                            );
                            break;
                          case "status": {
                            const statusInfo = getEntityStatusInfo(ad.status, ad.moderation_status);
                            content = (
                              <span className={`company-status ${statusInfo.className}`}>
                                <span className="status-dot" />
                                {statusInfo.text}
                              </span>
                            );
                            break;
                          }
                          case "goals":
                            content = formatNumber(stats?.base?.goals);
                            break;
                          case "cpa":
                            content = formatMoneyInt(stats?.base?.cpa);
                            break;
                          case "spent":
                            content = formatMoneyInt(stats?.base?.spent);
                            break;
                          case "clicks":
                            content = formatNumber(stats?.base?.clicks);
                            break;
                          case "shows":
                            content = formatNumber(stats?.base?.shows);
                            break;
                          case "created":
                            content = formatVkCreated(ad.created);
                            break;
                          case "adId":
                            content = ad.id;
                            break;
                          case "revenue":
                            content = formatNumber(getAdRevenue(ad.id));
                            break;
                          case "profit": {
                            const profit = getAdProfit(ad.id);
                            content = (
                              <span style={{ 
                                color: profit > 0 ? "#4caf50" : profit < 0 ? "#f44336" : "#888" 
                              }}>
                                {formatNumber(profit)}
                              </span>
                            );
                            break;
                          }
                        }

                        return (
                          <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                            {content}
                          </td>
                        );
                      })}
                    </tr>
                  );
                })}
              </tbody>
              <tfoot>
                <tr className="totals-row">
                  <td className="col-checkbox"></td>
                  <td className="col-toggle"></td>
                  {visibleColumns.map(col => {
                    let content: React.ReactNode = "";

                    switch (col.id) {
                      case "companyName":
                        content = <strong>Итого: {filteredAds.length} объявлений</strong>;
                        break;
                      case "goals":
                        content = <strong>{formatNumber(adsTotals.goals)}</strong>;
                        break;
                      case "cpa":
                        content = <strong>{formatMoneyInt(adsTotals.cpa)}</strong>;
                        break;
                      case "spent":
                        content = <strong>{formatMoneyInt(adsTotals.spent)}</strong>;
                        break;
                      case "clicks":
                        content = <strong>{formatNumber(adsTotals.clicks)}</strong>;
                        break;
                      case "shows":
                        content = <strong>{formatNumber(adsTotals.shows)}</strong>;
                        break;
                      case "revenue": {
                        const totalRevenue = filteredAds.reduce((sum, a) => sum + getAdRevenue(a.id), 0);
                        content = <strong>{formatNumber(totalRevenue)}</strong>;
                        break;
                      }
                      case "profit": {
                        const totalProfit = filteredAds.reduce((sum, a) => sum + getAdProfit(a.id), 0);
                        content = (
                          <strong style={{ 
                            color: totalProfit > 0 ? "#4caf50" : totalProfit < 0 ? "#f44336" : "#888" 
                          }}>
                            {formatNumber(totalProfit)}
                          </strong>
                        );
                        break;
                      }
                    }

                    return (
                      <td key={col.id} style={{ width: col.width, minWidth: col.width }}>
                        {content}
                      </td>
                    );
                  })}
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      </div>
    );
  };

  const renderCompaniesPage = () => {
    if (selectedCabinetId === "all") {
      return (
        <div className="content-section glass">
          <div className="section-header">
            <h2>Компании</h2>
          </div>
          <div className="hint">Выберите конкретный кабинет для просмотра компаний</div>
        </div>
      );
    }

    return (
      <div className="content-section glass companies-section">
        {/* View tabs with counts */}
        <div className="companies-view-tabs">
          <div className="companies-view-tabs-left">
            <button
              className={`companies-view-tab ${companiesViewTab === "campaigns" ? "active" : ""}`}
              onClick={() => setCompaniesViewTab("campaigns")}
            >
              Компании {selectedCounts.companies > 0 && <span className="tab-count">{selectedCounts.companies}</span>}
            </button>
            <button
              className={`companies-view-tab ${companiesViewTab === "groups" ? "active" : ""}`}
              onClick={() => setCompaniesViewTab("groups")}
            >
              Группы {selectedCounts.groups > 0 && <span className="tab-count">{selectedCounts.groups}</span>}
            </button>
            <button
              className={`companies-view-tab ${companiesViewTab === "ads" ? "active" : ""}`}
              onClick={() => setCompaniesViewTab("ads")}
            >
              Объявления {selectedCounts.ads > 0 && <span className="tab-count">{selectedCounts.ads}</span>}
            </button>
          </div>
          
          <div className="companies-view-tabs-right">
            <DateRangePicker
              dateFrom={companiesDateFrom}
              dateTo={companiesDateTo}
              onChange={handleDateChange}
              isOpen={datePickerOpen}
              onToggle={() => setDatePickerOpen(v => !v)}
              onApply={handleDateChange}
            />
          </div>
        </div>

        {companiesViewTab === "campaigns" && renderCompaniesTable()}
        {companiesViewTab === "groups" && renderGroupsTable()}
        {companiesViewTab === "ads" && renderAdsTable()}
      </div>
    );
  };

  const renderCampaignsHome = () => {
    if (campaignsSubTab === "companies") {
      return renderCompaniesPage();
    }
    return (
    <div className="content-section glass">
      <div className="section-header">
        <h2>Создание пресетов</h2>

        <div className="section-tools">
          <button
            className="outline-button"
            onClick={() => setViewMode(m => (m === "grid" ? "list" : "grid"))}
            title={viewMode === "grid" ? "Показать списком" : "Показать плиткой"}
          >
            {viewMode === "grid" ? "≣ Список" : "▦ Плитка"}
          </button>

          <div className="sort-group">
            <button
              className={`outline-button ${sortBy === "name" ? "active" : ""}`}
              onClick={() => {
                setSortBy("name");
                setSortDir(d => (sortBy === "name" ? (d === "asc" ? "desc" : "asc") : "asc"));
              }}
              title="Сортировать по названию"
            >
              По имени {sortBy === "name" ? (sortDir === "asc" ? "↑" : "↓") : ""}
            </button>
            
            <button
              className={`outline-button ${sortBy === "created" ? "active" : ""}`}
              onClick={() => {
                setSortBy("created");
                setSortDir(d => (sortBy === "created" ? (d === "asc" ? "desc" : "asc") : "desc"));
              }}
              title="Сортировать по дате"
            >
              По дате {sortBy === "created" ? (sortDir === "asc" ? "↑" : "↓") : ""}
            </button>
            
            <button
              className={`outline-button ${sortBy === "trigger" ? "active" : ""}`}
              onClick={() => {
                setSortBy("trigger");
                setSortDir(d => (sortBy === "trigger" ? (d === "asc" ? "desc" : "asc") : "asc"));
              }}
              title="Сортировать по времени триггера"
            >
              По триггеру {sortBy === "trigger" ? (sortDir === "asc" ? "↑" : "↓") : ""}
            </button>
          </div>
        </div>
      </div>
      <div className={`preset-grid ${viewMode === "list" ? "list" : ""}`}>
        {/* Сплит-карта создания показывается только в плитке */}
        {viewMode === "grid" && (
          <div className="preset-card split-card">
            <button className="split-area split-left" onClick={startNewPreset}>
              <span className="plus-icon">+</span>
              <span className="font-card">Новый пресет</span>
            </button>
            <button className="split-area split-right" onClick={startFastPreset}>
              <span className="plus-icon">⚡</span>
              <span className="font-card">Быстрый пресет</span>
            </button>
          </div>
        )}
      
        {sortedPresets.map((p) => {
          const title = p?.data?.company?.presetName || p?.preset_id || "Без имени";
          const isFastPreset = p.data?.fastPreset;
          const groupsCount = calcFastPresetGroupsCount(p.data);
          const adsCount = Array.isArray(p?.data?.ads) ? p.data.ads.length : 0;
          const trig = p?.data?.company?.time || "—:—";
          const created = formatPresetCreated(p?.created_at);
          const fast = !!p?.data?.fastPreset;
        
          const cardClass = `preset-card ${fast ? "fast" : ""} ${viewMode === "list" ? "row" : ""}`;
        
          return (
            <div
              key={p?.preset_id}
              className={cardClass}
              onClick={() => openPreset(p.preset_id, p.data)}
            >
              <div className="preset-name">
                <div className="preset-title-text" title={title}>
                  {title}
                </div>

                {viewMode === "list" && created && (
                  <span className="created-at">{created}</span>
                )}
              </div>
              
              <div className="preset-meta">
                <span>Групп: {groupsCount}</span>
                {/* Показываем "Объявлений" только для НЕ быстрых пресетов */}
                {!isFastPreset && <span>Объявлений: {adsCount}</span>}
              </div>
              
              <div className="preset-badges" onClick={(e) => e.stopPropagation()}>
                <span className="badge">{trig}</span>
                <div
                  role="switch"
                  aria-checked={queueStatus[p.preset_id] !== "deactive"}
                  title={queueStatus[p.preset_id] === "deactive"
                    ? "Выкл (не будет ставиться в очередь)"
                    : "Вкл"}
                  className={`toggle ${queueStatus[p.preset_id] === "deactive" ? "" : "on"}`}
                  onClick={() => togglePresetActive(p.preset_id)}
                >
                  <div className="toggle-knob" />
                </div>
              </div>
                  
              <div className="card-actions">
                <button
                  className="icon-button copy-button"
                  title="Копировать пресет"
                  onClick={(e) => { e.stopPropagation(); copyPreset(p); }}
                >
                  <IconCopy className="icon" />
                </button>

                <button
                  className="icon-button delete-button"
                  title="Удалить пресет"
                  onClick={(e) => { e.stopPropagation(); deletePreset(p.preset_id); }}
                >
                  <IconTrash className="icon" />
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
    );
  };

  const renderPresetStructure = () => {
    if (!presetDraft) return null;
    return (
      <div className="preset-structure">
        <button
          className={`structure-item ${
            selectedStructure.type === "company" ? "active" : ""
          }`}
          onClick={() => setSelectedStructure({ type: "company" })}
        >
          Компания
        </button>

        {presetDraft.groups.map((group, index) => (
          <div key={group.id} className="structure-group">
            <div className="structure-group-header">
              <button
                className={`structure-item child ${
                  selectedStructure.type === "group" &&
                  selectedStructure.index === index
                    ? "active"
                    : ""
                }`}
                onClick={() =>
                  setSelectedStructure({ type: "group", index })
                }
              >
                ⤷ Группа {index + 1}
              </button>
              {!presetDraft?.fastPreset && (
                <div className="structure-actions">
                  <button
                    className="icon-button"
                    title="Дублировать группу"
                    onClick={() => cloneGroup(index)}
                  >
                    <IconCopy className="icon" />
                  </button>
                  <button
                    className="icon-button"
                    title="Удалить группу"
                    onClick={() => deleteGroup(index)}
                  >
                    <IconTrash className="icon" />
                  </button>
                </div>
              )}
            </div>

            <button
              className={`structure-item child deeper ${
                selectedStructure.type === "ad" &&
                selectedStructure.index === index
                  ? "active"
                  : ""
              }`}
              onClick={() =>
                setSelectedStructure({ type: "ad", index })
              }
            >
              &nbsp;&nbsp;⤷ Объявление
            </button>
          </div>
        ))}
      </div>
    );
  };

  const renderCompanySettings = () => {
    if (!presetDraft) return null;
    const company = presetDraft.company;

    const updateCompany = (patch: Partial<PresetCompany>) =>
      setPresetDraft({ ...presetDraft, company: { ...company, ...patch } });

    return (
      <div className="form-grid two-col">
        <div className="form-field">
          <label>Название пресета</label>
          <input
            type="text"
            value={company.presetName}
            onChange={(e) =>
              updateCompany({ presetName: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>Название кампании</label>
          <input
            type="text"
            value={company.companyName}
            onChange={(e) =>
              updateCompany({ companyName: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>Целевое действие</label>
          <select
            value={company.targetAction}
            onChange={(e) => {
              const v = e.target.value;

              // Формируем единый патч: ставим новую цель и при leadads очищаем пиксель/URL
              const patch: Partial<PresetCompany> = { targetAction: v };
              if (v === "leadads") {
                patch.sitePixel = "";
                patch.url = "";
                // если хотите — сразу чистим и leadform_id: patch.leadform_id = "";
              }

              updateCompany(patch);
            }}
          >
            <option value="">Не выбрано</option>
            <option value="socialengagement">Сообщение в группу</option>
            <option value="site_conversions">На сайт</option>
            <option value="leadads">Лидформа</option>
          </select>

        </div>
        {company.targetAction === "site_conversions" && (
          <div className="form-field">
            <label>Действие</label>
            <select
              value={company.siteAction ?? "uss:success"}
              onChange={(e) => updateCompany({ siteAction: e.target.value })}
            >
              <option value="uss:success">Оформление заявки лид</option>

              {userId === "1342381428" && (
                <option value="jse:pro_click">JS событие - pro_click</option>
              )}
            </select>
          </div>
        )}
        {company.targetAction === "site_conversions" && (
          <div className="form-field" style={{ maxWidth:330 }}>
            <label>Пиксель сайта</label>

            <PixelSelect
              pixels={sitePixels}
              value={company.sitePixel ?? ""}
              disabled={selectedCabinetId === "all"}
              placeholder={selectedCabinetId === "all" ? "Выберите кабинет (не all)" : "Начните вводить пиксель/домен"}
              onSelect={(it) => {
                updateCompany({ sitePixel: it.pixel, url: it.domain }); // домен → в URL
              }}
              onAdd={async () => {
                const res = await askPixelInput();
                if (!res) return;
              
                const next: SitePixel[] = [
                  ...sitePixels.filter(p => !(p.pixel === res.pixel)), // если такой уже был — заменим
                  { pixel: res.pixel, domain: res.domain }
                ];
                await savePixels(next);       // твоя функция сохранения на бэк
                setSitePixels(next);
              
                updateCompany({ sitePixel: res.pixel, url: res.domain });
              }}
              onDelete={async (it) => {
                if (!(await askConfirm(`Удалить пиксель "${it.pixel}"?`))) return;
              
                const next = sitePixels.filter(p => !(p.pixel === it.pixel));
                await savePixels(next);
                setSitePixels(next);
              
                if ((company.sitePixel ?? "") === it.pixel) {
                  updateCompany({ sitePixel: "", url: "" });
                }
              }}
            />

            {/* необязательно, но удобно видеть что подставилось */}
            {!!company.url && (
              <div className="hint" style={{ marginTop: 6 }}>
                Домен: {company.url}
              </div>
            )}
          </div>
        )}

        {company.targetAction === "site_conversions" && (
          <div className="form-field">
            <label>Ссылка на сайт</label>
            <input
              type="text"
              placeholder="https://example.com/landing"
              value={company.bannerUrl ?? ""}
              onChange={(e) => updateCompany({ bannerUrl: e.target.value })}
            />
          </div>
        )}

        {company.targetAction === "socialengagement" && (
          <div className="form-field">
            <label>URL</label>
            <input
              type="text"
              placeholder="Ссылка на группу"
              value={company.url ?? ""}
              onChange={(e) => updateCompany({ url: e.target.value })}
            />
          </div>
        )}

        {company.targetAction === "leadads" && (
          <div className="form-field">
            <label>Лидформа</label>

            <LeadFormSelect
              leadForms={leadForms}
              value={company.leadform_id ?? ""}
              disabled={selectedCabinetId === "all"}
              placeholder={selectedCabinetId === "all" ? "Выберите кабинет (не all)" : "Выберите лидформу"}
              onSelect={(lf) => {
                updateCompany({ leadform_id: lf.id, url: "" });
              }}
              onRefresh={() => loadLeadForms(true)}
            />
          </div>
        )}

        <div className="form-field">
          <label>Триггер</label>
          <select
            value={company.trigger}
            onChange={(e) => {
              const val = e.target.value;
              if (val.startsWith("trigger_preset_")) {
                updateCompany({ trigger: val, time: "custom" });
              } else {
                updateCompany({ trigger: val });
              }
            }}
          >
            <option value="time">Время</option>
            {triggerPresets.length > 0 && (
              <optgroup label="Пользовательские триггеры">
                {triggerPresets.map((tp) => (
                  <option key={tp.id} value={tp.id}>
                    {tp.name}
                  </option>
                ))}
              </optgroup>
            )}
          </select>
        </div>
        {company.trigger === "time" && (
          <div className="form-field">
            <label>Время</label>
            <input
              type="time"
              value={company.time || ""}
              onChange={(e) => updateCompany({ time: e.target.value })}
            />
          </div>
        )}
        {company.trigger?.startsWith("trigger_preset_") && (
          <div className="form-field">
            <div className="hint" style={{ marginTop: 4 }}>
              Используется пользовательский триггер: {triggerPresets.find(tp => tp.id === company.trigger)?.name || company.trigger}
            </div>
          </div>
        )}
        <div className="form-field">
          <label>Кол-во дублей</label>
          <input
            type="number"
            min={1}
            value={company.duplicates ?? 1}
            onChange={(e) =>
              updateCompany({ duplicates: Math.max(1, parseInt(e.target.value || "1", 10)) })
            }
          />
        </div>
      </div>
    );
  };

  const renderGroupSettings = () => {
    if (!presetDraft) return null;
    const isFast = !!presetDraft.fastPreset;
    const index = selectedStructure.index ?? 0;
    const group = presetDraft.groups[index];
    const companyTarget = presetDraft.company.targetAction;

    const updateGroup = (patch: Partial<PresetGroup>) => {
      const groups = [...presetDraft.groups];
      groups[index] = { ...group, ...patch };
      setPresetDraft({ ...presetDraft, groups });
    };

    return (
      <div className="form-grid preset-group-grid">
        <div className="form-field">
          <label>Название группы</label>
          <input
            type="text"
            value={group.groupName}
            onChange={(e) => updateGroup({ groupName: e.target.value })}
          />
        </div>
        <div className="form-field">
          <label>Стратегия ставок</label>
          <select
            value={group.bidStrategy ?? "min"}
            onChange={(e) => {
              const v = e.target.value as "min" | "cap";
              updateGroup({
                bidStrategy: v,
                ...(v === "min" ? { maxCpa: "" } : {}),
              });
            }}
          >
            <option value="min">Минимальная цена</option>
            <option value="cap">Предельная цена</option>
          </select>
        </div>

        {group.bidStrategy === "cap" && (
          <div className="form-field">
            <label>Макс. стоимость конверсии</label>
            <input
              type="number"
              min={0}
              value={group.maxCpa ?? ""}
              onChange={(e) => updateGroup({ maxCpa: e.target.value })}
            />
          </div>
        )}
        <div className="form-field">
          <label>Бюджет</label>
          <input
            type="text"
            min={100}
            value={group.budget}
            onChange={(e) => updateGroup({ budget: e.target.value })}
          />
        </div>
        {companyTarget !== "leadads" && (
          <div className="form-field">
            <label>UTM</label>
            <input
              type="text"
              value={group.utm}
              onChange={(e) => updateGroup({ utm: e.target.value })}
            />
          </div>
        )}
        <div className="form-field">
          <label>Регионы</label>
          <RegionsTreeSelect
            selected={group.regions}
            onChange={(arr) => {
              let next = [...arr];
            
              const hasWorldwide = next.includes(-1);
              const hasMinus = next.some(v => v < 0 && v !== -1); // исключённые регионы (не считая -1)
              const hasPlusOtherThan188 = next.some(v => v > 0 && v !== 188);
            
              if (hasWorldwide) {
                // Режим «Весь мир»: разрешаем -1 + любые минусы (исключённые регионы внутри России)
                next = next.filter(v => v === -1 || (v < 0 && v !== -1));
                if (!next.includes(-1)) next = [-1, ...next];
              } else if (hasMinus) {
                // Режим «исключать»: разрешаем 188 + любые минусы
                next = next.filter(v => v < 0 || v === 188);
                if (!next.includes(188)) next = [188, ...next]; // страховка
              } else {
                // Режим «включать»: только плюсы, без минусов; 188 убираем, если есть другие плюсы
                next = next.filter(v => v > 0);
                if (hasPlusOtherThan188) next = next.filter(v => v !== 188);
                if (next.length === 0) next = [188];
              }
            
              updateGroup({ regions: next });
            }}
          />
        </div>
        <div className="form-field">
          <label>Пол</label>
          <select
            value={group.gender}
            onChange={(e) =>
              updateGroup({
                gender: e.target.value as PresetGroup["gender"],
              })
            }
          >
            <option value="male,female">Любой</option>
            <option value="male">Мужской</option>
            <option value="female">Женский</option>
          </select>
        </div>
        <div className="form-field">
          <label>Возраст</label>
          <input
            type="text"
            placeholder="21-55"
            value={group.age}
            onChange={(e) => updateGroup({ age: e.target.value })}
          />
        </div>
        {(companyTarget === "socialengagement" || companyTarget === "site_conversions") && (
          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <label>Места размещения</label>
            <PlacementsTreeSelect
              targetAction={companyTarget}
              selected={group.placements || []}
              onChange={(arr) => updateGroup({ placements: arr })}
            />
          </div>
        )}
        <div className="form-field">
          <label>Интересы</label>
          <InterestsTreeSelect
            selected={group.interests}
            onChange={(arr) => updateGroup({ interests: arr })}
          />
        </div>
        {isFast ? (
          <div className="form-field" style={{ gridColumn: "1 / -1" }}>
            <label>Контейнеры аудиторий</label>
            <div className="container-list" style={{display:"flex", flexDirection:"column", gap:12}}>
              {(group.containers || []).map((ct, ci) => (
                <div key={ct.id} className="glass" style={{padding:12, borderRadius:12}}>
                  <div style={{display:"flex", gap:8, alignItems:"center", marginBottom:8}}>
                    <input
                      type="text"
                      value={ct.name}
                      placeholder={`Контейнер ${ci+1}`}
                      onChange={(e)=>{
                        const next = [...(group.containers||[])];
                        next[ci] = { ...ct, name: e.target.value };
                        updateGroup({ containers: next });
                      }}
                    />
                    <button className="icon-button" title="Удалить контейнер" onClick={()=>{
                      const next = [...(group.containers||[])];
                      next.splice(ci,1);
                      updateGroup({ containers: next });
                    }}><IconTrash className="icon" /></button>
                  </div>
                  
                  <AudiencesMultiSelect
                    apiBase={API_BASE}
                    userId={userId!}
                    cabinetId={selectedCabinetId}
                    vkAudiences={audiences}
                    abstractAudiences={abstractAudiences}
                    selectedVkIds={ct.audienceIds}
                    selectedVkNames={ct.audienceNames || []}
                    selectedAbstractNames={ct.abstractAudiences || []}
                    onChange={({ vkIds, vkNames, abstractNames })=>{
                      const names = vkNames.slice(0, vkIds.length);
                      while (names.length < vkIds.length) names.push("Аудитория");
                      const next = [...(group.containers||[])];
                      next[ci] = {
                        ...ct,
                        audienceIds: vkIds,
                        audienceNames: names,
                        abstractAudiences: abstractNames
                      };
                      updateGroup({ containers: next });
                    }}
                  />
                </div>
              ))}

              <button
                className="outline-button"
                onClick={()=>{
                  const next = [...(group.containers||[]), {
                    id: generateId(),
                    name: `Контейнер ${ (group.containers?.length || 0) + 1 }`,
                    audienceIds: [],
                    audienceNames: [],
                    abstractAudiences: [],
                  } as AudienceContainer];
                  updateGroup({ containers: next });
                }}
              >
                + Добавить контейнер
              </button>
            </div>
          </div>
        ) : (
          /* СТАРЫЙ блок аудиторий для обычного пресета — как у вас было */
          <div className="form-field">
            <label>Аудитории</label>
            <AudiencesMultiSelect
              apiBase={API_BASE}
              userId={userId!}
              cabinetId={selectedCabinetId}
              vkAudiences={audiences}
              abstractAudiences={abstractAudiences}
              selectedVkIds={group.audienceIds}
              selectedVkNames={group.audienceNames ?? []}
              selectedAbstractNames={group.abstractAudiences ?? []}
              onChange={({ vkIds, vkNames, abstractNames }) => {
                const names = vkNames.slice(0, vkIds.length);
                while (names.length < vkIds.length) names.push("Аудитория");
                updateGroup({
                  audienceIds: vkIds,
                  audienceNames: names,
                  abstractAudiences: abstractNames
                });
              }}
            />
          </div>
        )}
      </div>
    );
  };

  const renderAdSettings = () => {
    if (!presetDraft) return null;
    const index = selectedStructure.index ?? 0;
    const ad = presetDraft.ads[index];

    const updateAd = (patch: Partial<PresetAd>) => {
      const ads = [...presetDraft.ads];
      ads[index] = { ...ad, ...patch };
      setPresetDraft({ ...presetDraft, ads });
    };

    const selectedVideos = ad.videoIds
      .map(vid => findItemByAnyId(vid, creativeSets, selectedCabinetId))
      .filter(Boolean) as CreativeItem[];
    const selectedImages = (ad.imageIds || [])
      .map(vid => findItemByAnyId(vid, creativeSets, selectedCabinetId))
      .filter(Boolean) as CreativeItem[];
    const companyTarget = presetDraft.company.targetAction;
    const selectedCreativeSets = ad.creativeSetIds
      .map((id) => creativeSets.find((s) => s.id === id) || null)
      .filter(Boolean) as CreativeSet[];

    return (
      <div className="form-grid two-col">
        <div className="form-field">
          <label>Название объявления</label>
          <input
            type="text"
            value={ad.adName}
            onChange={(e) => updateAd({ adName: e.target.value })}
          />
        </div>
        <div className="form-field">
          <label>Текстовый набор</label>
          <select
            value={ad.textSetId ?? ""}
            onChange={(e) => {
              const val = e.target.value;
              if (val === "new") {
                const tempId = generateId();
                updateAd({
                  textSetId: tempId,
                  isNewTextSet: true,
                  newTextSetName: "",
                  title: "",
                  shortDescription: "",
                  longDescription: "",
                });
              } else if (!val) {
                updateAd({
                  textSetId: null,
                  isNewTextSet: false,
                  newTextSetName: "",
                  title: "",
                  shortDescription: "",
                  longDescription: "",
                });
              } else {
                // выбран существующий набор
                const s = textSets.find(ts => ts.id === val);
                const textsetLogo = (s as any)?.logoId || "";
                updateAd({
                  textSetId: val,
                  isNewTextSet: false,
                  newTextSetName: s?.name ?? "",
                  title: s?.title ?? "",
                  shortDescription: s?.shortDescription ?? "",
                  longDescription: s?.longDescription ?? "",
                  advertiserInfo: (s as any)?.advertiserInfo ?? "",
                  logoId: textsetLogo || (logo?.id ?? ""),
                  button: (s as any)?.button ?? "",
                });
              }
            }}
          >
            <option value="">Не выбран</option>
            <option value="new">Создать новый</option>
            {textSets.map(ts => (
              <option key={ts.id} value={ts.id}>{ts.name}</option>
            ))}
          </select>
        </div>
          
        {/* Поля редактирования */}
        {ad.textSetId && (
          <>
            <div className="form-field">
              <label>Название текстового набора</label>
              <input
                type="text"
                value={ad.newTextSetName}
                onChange={(e) => updateAd({ newTextSetName: e.target.value })}
              />
            </div>
        
            <div className="form-field">
              <label>Заголовок</label>
              <input
                type="text"
                value={ad.title ?? ""}
                onChange={(e) => updateAd({ title: e.target.value })}
              />
            </div>
        
            <div className="form-field">
              <label>Короткое описание</label>
              <textarea
                rows={2}
                value={ad.shortDescription}
                onChange={(e) => {
                  const ta = e.currentTarget;
                  updateAd({ shortDescription: ta.value });
                  ta.style.height = "auto";
                  ta.style.height = ta.scrollHeight + "px";
                }}
                ref={(el) => {
                  if (!el) return;
                  requestAnimationFrame(() => {
                    el.style.height = "auto";
                    el.style.height = el.scrollHeight + "px";
                  });
                }}
                style={{ overflow: "hidden", resize: "none" }}
              />
            </div>
        
            {companyTarget !== "socialengagement" && (
              <div className="form-field">
                <label>Длинное описание</label>
                <textarea
                  rows={4}
                  value={ad.longDescription}
                  onChange={(e) => updateAd({ longDescription: e.target.value })}
                />
              </div>
            )}

            <div className="form-field">
              <label>Кнопка</label>
              <select
                value={ad.button ?? ""}
                onChange={(e) => updateAd({ button: e.target.value })}
              >
                <option value="">Не выбрано</option>
                {(companyTarget === "leadads" ? CTA_OPTIONS_LEADADS : CTA_OPTIONS).map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>

            {companyTarget === "leadads" && (
              <div className="form-field">
                <label>Текст на кнопке</label>
                <input
                  type="text"
                  maxLength={30}
                  placeholder="Дополнительный текст на кнопке"
                  value={ad.buttonText ?? ""}
                  onChange={(e) => updateAd({ buttonText: e.target.value })}
                />
              </div>
            )}

            {ad.textSetId && (
              <>
                <div className="form-field">
                  <label>Данные о рекламодателе</label>
                  <textarea
                    rows={2}
                    value={ad.advertiserInfo ?? textSets.find(ts => ts.id === ad.textSetId)?.advertiserInfo ?? ""}
                    onChange={(e) => updateAd({ advertiserInfo: e.target.value })}
                  />
                </div>

                <div className="form-field">
                  <label>Логотип</label>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    <button
                      type="button"
                      title={logo ? "Использовать этот логотип" : "Загрузите логотип во вкладке «Логотип»"}
                      className="icon-button"
                      onClick={async () => {
                        if (!logo?.id) { setActiveTab("logo"); showPopup("Сначала загрузите логотип"); return; }
                      
                        // 1) проставим в объявление
                        updateAd({ logoId: logo.id });
                      
                        // 2) если выбран существующий textset — обновим его logoId и сохраним
                        if (ad.textSetId && !ad.isNewTextSet) {
                          const exists = textSets.some(ts => ts.id === ad.textSetId);
                          if (exists) {
                            const next = textSets.map(ts =>
                              ts.id === ad.textSetId ? { ...ts, logoId: logo.id } : ts
                            );
                            try {
                              await upsertTextSets(next);
                              showPopup("Логотип обновлён в текстовом наборе");
                            } catch (e) {
                              console.warn(e);
                              showPopup("Не удалось обновить логотип в textset");
                            }
                          }
                        }
                      }}
                      style={{ width: 60, borderRadius: "50%", overflow: "hidden", border: "1px solid var(--border-color)", display: "flex", alignItems: "center", justifyContent: "center", background: "var(--bg-muted)" }}
                    >
                      {logo?.url ? <img src={logo.url} alt="logo" style={{ width: "100%", height: "100%", objectFit: "cover" }} /> : "＋"}
                    </button>
                    <div style={{ opacity: 0.8, fontSize: 12 }}>
                      {ad.logoId ? `Выбран логотип id: ${ad.logoId}` : "Логотип не выбран"}
                    </div>
                  </div>
                </div>
              </>
            )}
          </>
        )}
        <div className="form-field">
          <label>Выбрать видео</label>
          <div className="video-picker-field">
            <button
              type="button"
              className="outline-button"
              onClick={() => openVideoPickerForAd(ad.id)}
            >
              Открыть список креативов
            </button>

            <div className="selected-videos">
              {selectedVideos.length === 0 &&
                selectedCreativeSets.length === 0 && (
                  <span className="hint">
                    Видео пока не выбраны. Перейдите во вкладку
                    «Креативы», создайте набор и загрузите ролики.
                  </span>
                )}

              {selectedCreativeSets.length > 0 && (
                <div className="selected-group">
                  <div className="selected-title">Выбранные наборы:</div>
                  <div className="pill-list">
                    {selectedCreativeSets.map((set) => (
                      <span key={set.id} className="pill active">
                        {set.name}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {selectedImages.length > 0 && (
                <div className="selected-group">
                  <div className="selected-title">Изображения:</div>
                  <div className="video-chip-list">
                    {selectedImages.map((img) => (
                      <div key={img!.id} className="video-chip">
                        <div className="thumb" />
                        <span>{img!.name}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {selectedVideos.length > 0 && (
                <div className="selected-group">
                  <div className="selected-title">Отдельные видео:</div>
                  <div className="video-chip-list">
                    {selectedVideos.map((v) => (
                      <div key={v!.id} className="video-chip">
                        <div className="thumb" />
                        <span>{v!.name}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    );
  };

  const renderPresetEditor = () => (
    <div className={`preset-editor glass ${presetDraft?.fastPreset ? "fast" : ""}`}>
      <div className="preset-editor-left">{renderPresetStructure()}</div>
      <div className="preset-editor-right">
        {selectedStructure.type === "company" && renderCompanySettings()}
        {selectedStructure.type === "group" && renderGroupSettings()}
        {selectedStructure.type === "ad" && renderAdSettings()}

        <div className="preset-actions">
          <button
            className="primary-button"
            onClick={savePreset}
            disabled={saving}
          >
            {saving ? "Сохранение..." : "Сохранить пресет"}
          </button>
          <button
            className="outline-button"
            onClick={() => {
              setPresetDraft(null);
              setView({ type: "home" });
            }}
          >
            Отмена
          </button>
        </div>
      </div>
    </div>
  );

  const renderCreativesPage = () => (
    <div className="content-section glass">
      <div className="section-header">
        <h2>Креативы</h2>
        <button className="primary-button" onClick={createCreativeSet}>
          + Новый набор
        </button>
      </div>

      <div className="creative-layout">
        <div className="creative-sets-list">
          {creativeSets.map((set) => (
            <button
              key={set.id}
              className={`creative-set-item ${
                selectedCreativeSetId === set.id ? "active" : ""
              }`}
              onClick={() => setSelectedCreativeSetId(set.id)}
            >
              <div className="title">{set.name}</div>
              <div className="meta">{set.items.length} файлов</div>
              <button
                className="icon-button delete-button"
                onClick={(e) => {
                  e.stopPropagation();
                  deleteCreativeSet(set.id);
                }}
              >
                <IconTrash className="icon" />
              </button>
            </button>
          ))}
          {creativeSets.length === 0 && (
            <div className="hint">
              Пока нет наборов. Создайте первый набор креативов.
            </div>
          )}
        </div>

        <div className="creative-set-content">
          {currentCreativeSet ? (
            <div className="creative-set-inner">
              <div className="creative-set-header">
                <input
                  className="creative-set-name-input"
                  value={currentCreativeSet.name}
                  onChange={(e) =>
                    renameCreativeSet(
                      currentCreativeSet.id,
                      e.target.value
                    )
                  }
                />
                <label className="upload-button">
                  <input
                    type="file"
                    multiple
                    accept="video/*,image/*"
                    onChange={(e) =>
                      uploadCreativeFiles(e.target.files)
                    }
                  />
                  Перетащите или выберите файлы
                </label>
              </div>

              <div className="creative-grid">
                {uploadingCount > 0 && Array.from({ length: uploadingCount }).map((_, i) => (
                  <div key={`ph_${i}`} className="creative-card skeleton">
                    <div className="creative-thumb skeleton-anim" />
                    <div className="creative-name skeleton-anim" style={{ height: 14, marginTop: 8 }} />
                  </div>
                ))}
                {currentCreativeSet.items.slice(0, creativeLimit).map((item) => {
                  const realUrl =
                    item.urls?.[selectedCabinetId] ??
                    item.url ?? "";
                                
                  return (
                    <div key={item.id} className="creative-card" onContextMenu={(e) => openContextMenuForItem(e, currentCreativeSet.id, item.id)}>
                      {item.uploaded && (
                        <div className="creative-checkmark">✔</div>
                      )}
                
                      {item.type === "image" ? (
                        <img
                          src={realUrl}
                          alt={item.name}
                          className="creative-thumb"
                          loading="lazy"
                        />
                      ) : (
                        <img
                          src={item.thumbUrl || realUrl}
                          alt={item.name}
                          className="creative-thumb"
                          loading="lazy"
                        />
                      )}
                
                      <div className="creative-name">{item.name}</div>
                    
                      <button
                        className="icon-button delete-button"
                        onClick={() =>
                          deleteCreativeItem(currentCreativeSet.id, item.id)
                        }
                      >
                        <IconTrash className="icon" />
                      </button>
                    </div>
                  );
                })}
                {creativeLimit < currentCreativeSet.items.length && (
                  <div ref={creativesSentinelRef} style={{ height: 1 }} />
                )}
                {currentCreativeSet.items.length === 0 && uploadingCount === 0 && (
                  <div className="hint">Загрузите видео или картинки</div>
                )}
              </div>
            </div>
          ) : (
            <div className="hint">
              Выберите набор слева или создайте новый.
            </div>
          )}
        </div>
      </div>
    </div>
  );
  const renderLogoPage = () => {
    const onUpload = async (files: FileList | null) => {
      if (!files || !files[0] || !userId) return;
      setLogoLoading(true);
      try {
        const form = new FormData();
        form.append("file", files[0]);
        const resp = await fetchSecured(
          `${API_BASE}/logo/upload?user_id=${encodeURIComponent(userId!)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`,
          { method: "POST", body: form }
        );
        const j = await resp.json();
        if (!resp.ok || !j.logo) throw new Error(j.error || "Upload error");
        setLogo(j.logo);
        setPresetDraft(prev => (prev ? applyDefaultLogoToDraft(prev, j.logo?.id) : prev));
        showPopup("Логотип загружен");
      } catch (e: any) {
        console.error(e);
        showPopup(e.message || "Ошибка загрузки логотипа");
      } finally {
        setLogoLoading(false);
      }
    };

    return (
      <div className="content-section glass">
        <div className="section-header">
          <h2>Логотип</h2>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div
            style={{
              width: 96, height: 96,
              borderRadius: "50%",
              overflow: "hidden",
              border: "1px solid var(--border-color)",
              display: "flex", alignItems: "center", justifyContent: "center",
              background: "var(--bg-muted)"
            }}
          >
            {logo?.url ? (
              <img src={logo.url} alt="logo" style={{ width: "100%", height: "100%", objectFit: "cover" }} />
            ) : (
              <span style={{ opacity: 0.7 }}>256×256</span>
            )}
          </div>

          <label className="upload-button">
            <input type="file" accept="image/*" onChange={(e) => onUpload(e.target.files)} />
            {logoLoading ? "Загрузка…" : "Выбрать изображение"}
          </label>
        </div>

        <div className="hint" style={{ marginTop: 12 }}>
          Изображение обрежется до квадрата 256×256, затем загрузится в VK.
        </div>
      </div>
    );
  };

  const renderAudiencesPage = () => {
    const loadFromVK = async () => {
      if (selectedCabinetId === "all") {
        showPopup("Выберите конкретный кабинет");
        return;
      }
      setLoading(true);
      const json = await apiJson(
        `${API_BASE}/vk/audiences/fetch?user_id=${userId}&cabinet_id=${selectedCabinetId}`
      );
      setLoading(false);

      if (json.error) showPopup(json.error);
      else setAudiences(json.audiences || []);
    };

    const saveAbstract = async (items: { name: string }[]) => {
      await fetchSecured(`${API_BASE}/abstract_audiences/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          audiences: items,
        }),
      });
      setAbstractAudiences(items);
    };

    const addAbstract = () => {
      if (!newAbstractName.trim()) return;
      const next = [...abstractAudiences, { name: newAbstractName.trim() }];
      saveAbstract(next);
      setNewAbstractName("");
    };

    const deleteAbstract = (name: string) => {
      const next = abstractAudiences.filter((a) => a.name !== name);
      saveAbstract(next);
    };

    const sortedVk = [...audiences].sort((a, b) => {
      const dt = tsFromCreated(b.created) - tsFromCreated(a.created);
      if (dt !== 0) return dt;
      const ai = parseInt(a.id, 10),
        bi = parseInt(b.id, 10);
      if (!Number.isNaN(ai) && !Number.isNaN(bi)) return bi - ai; // запасной вариант
      return b.id.localeCompare(a.id);
    });

    return (
      <div className="content-section glass">
        <div className="section-header audiences-header">
          <div className="aud-tabs">
            <button
              className={`aud-tab ${audTab === "vk" ? "active" : ""}`}
              onClick={() => setAudTab("vk")}
              type="button"
            >
              Аудитория VK
            </button>

            <button
              className={`aud-tab ${audTab === "lists" ? "active" : ""}`}
              onClick={() => setAudTab("lists")}
              type="button"
            >
              Списки
            </button>

            <button
              className={`aud-tab ${audTab === "templates" ? "active" : ""}`}
              onClick={() => setAudTab("templates")}
              type="button"
            >
              Шаблоны аудитории
            </button>
          </div>

          <div className="audiences-header-right">
            {audTab === "vk" && selectedCabinetId !== "all" && (
              <button className="primary-button" onClick={loadFromVK}>
                Обновить из VK
              </button>
            )}
          </div>
        </div>

        {/* Вкладка ШАБЛОНЫ (абстрактные аудитории) */}
        {audTab === "templates" && (
          <>
            <h3>Абстрактные аудитории</h3>

            <div style={{ display: "flex", gap: "8px" }}>
              <input
                placeholder="Название"
                value={newAbstractName}
                onChange={(e) => setNewAbstractName(e.target.value)}
              />
              <button className="primary-button" onClick={addAbstract}>
                +
              </button>
            </div>

            <div className="pill-select" style={{ marginTop: "10px" }}>
              {abstractAudiences.map((a) => (
                <div
                  key={a.name}
                  className="pill active"
                  style={{
                    display: "flex",
                    gap: "6px",
                    alignItems: "center",
                    fontSize: 16,
                  }}
                >
                  {a.name}
                  <button
                    className="icon-button"
                    onClick={() => deleteAbstract(a.name)}
                  >
                    <IconTrash className="icon" />
                  </button>
                </div>
              ))}
              {abstractAudiences.length === 0 && (
                <div className="hint" style={{ marginTop: 10 }}>
                  Пока нет шаблонов аудиторий.
                </div>
              )}
            </div>
          </>
        )}

        {/* Вкладка АУДИТОРИИ VK */}
        {audTab === "vk" && (
          <>
            {selectedCabinetId !== "all" ? (
              <>
                <h3 style={{ marginTop: "20px" }}>Аудитории VK</h3>
                <div className="pill-select column">
                  {sortedVk.map((a) => (
                    <div key={a.id} className="pill one-row">
                      <div
                        style={{
                          display: "flex",
                          flexDirection: "column",
                        }}
                      >
                        <span
                          style={{
                            fontWeight: 600,
                            fontSize: 16,
                          }}
                        >
                          {a.name}
                        </span>
                      </div>
                    </div>
                  ))}
                  {sortedVk.length === 0 && (
                    <div className="hint" style={{ marginTop: 10 }}>
                      Список аудиторий пуст. Нажмите «Обновить из VK».
                    </div>
                  )}
                </div>
              </>
            ) : (
              <div className="hint" style={{ marginTop: "10px" }}>
                Для VK-аудиторий выберите конкретный кабинет.
              </div>
            )}
          </>
        )}

        {/* Вкладка СПИСКИ */}
        {audTab === "lists" && (
          <>
            {selectedCabinetId === "all" || !userId ? (
              <div className="hint" style={{ marginTop: 10 }}>
                Для работы со списками выберите конкретный кабинет.
              </div>
            ) : (
              <UsersListsTab userId={userId} cabinetId={selectedCabinetId} />
            )}
          </>
        )}
      </div>
    );
  };

  const renderHistoryPage = () => {
    const toggleOpen = (idx: number) => {
      setOpenedHistoryIds(prev => ({ ...prev, [String(idx)]: !prev[String(idx)] }));
    };
  
    // Фильтруем по выбранной дате
    const filteredByDate = history.filter(it => {
      const itemDate = getHistoryDateStr(it?.date_time);
      return itemDate === historyDate;
    });
  
    // свежие сверху
    const items = [...filteredByDate].sort((a, b) => {
      const ta = parseHistoryTS(a?.date_time);
      const tb = parseHistoryTS(b?.date_time);
      return tb - ta;
    });
  
    return (
      <div className="content-section glass">
        <div className="section-header">
          <h2>История запусков</h2>
          
          {/* Выбор даты */}
          <div className="history-date-picker">
            <input
              type="date"
              value={historyDate}
              onChange={(e) => setHistoryDate(e.target.value)}
            />
          </div>
        </div>
    
        {items.length === 0 && (
          <div className="hint">Нет записей за {getDateLabel(historyDate).toLowerCase()}</div>
        )}
  
        <div className="history-list">
          {items.map((it, idx) => {
            const ok = it?.status === "success";
            const dateTime = formatHistoryDateTime(it?.date_time); // Новый формат
            const isOpen = !!openedHistoryIds[String(idx)];
          
            return (
              <div
                key={`${String(it?.preset_id ?? "")}_${idx}`}
                className={`history-card ${ok ? "ok" : "err"} ${isOpen ? "open" : ""}`}
                onClick={() => toggleOpen(idx)}
                title={it.date_time}
              >
                <div className="history-row">
                  <div className="history-title">
                    <span className="bullet" />
                    <span className="name">{it?.preset_name || "Без названия"}</span>
                    <span className="status">{ok ? " — Успешно" : " — Ошибка"}</span>
                  </div>
                  <div className="history-meta">
                    <span className="time">{dateTime}</span> {/* Теперь полная дата */}
                    <span className="sep">•</span>
                    <span className="trig">Триггер: {it?.trigger_time || "-"}</span>
                  </div>
                  <div className="drop-icon">{isOpen ? "▾" : "▸"}</div>
                </div>
            
                {isOpen && (
                  <div className="history-details">
                    {!ok && it?.text_error && it.text_error !== "null" && (
                      <div className="error-text">{String(it.text_error)}</div>
                    )}
  
                    {ok && Array.isArray(it.id_company) && it.id_company.length > 0 && (
                      <div className="ids">
                        ID групп: {it.id_company.join(", ")}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  const renderVideoPickerDrawer = () => {
    if (!videoPicker.open || !videoPicker.adId || !presetDraft) return null;
    const ad = getAdById(videoPicker.adId);
    if (!ad) return null;

    const isItemSelected = (item: CreativeItem) =>
      item.type === "image"
        ? (ad.imageIds || []).some(vid =>
        videoIdMatchesItem(vid, item, selectedCabinetId)
          )
        : (ad.videoIds || []).some(vid =>
        videoIdMatchesItem(vid, item, selectedCabinetId)
          );

    const isSetSelected = (id: string) =>
      ad.creativeSetIds.includes(id);

    return (
      <div className="drawer-backdrop" onClick={closeVideoPicker}>
        <div
          className="drawer"
          onClick={(e) => e.stopPropagation()}
        >
          <div className="drawer-header">
            <div className="drawer-title">Выбор креативов</div>
            <button className="icon-button" onClick={closeVideoPicker}>
              ✕
            </button>
          </div>
          <div className="drawer-content" ref={drawerScrollRef}>
            {creativeSets.length === 0 && (
              <div className="hint">
                Наборов креативов пока нет. Создайте их во вкладке
                «Креативы».
              </div>
            )}

            {creativeSets.map((set) => (
              <div key={set.id} className="drawer-set">
                <div className="drawer-set-header">
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={isSetSelected(set.id)}
                      onChange={() =>
                        toggleCreativeSetForAd(ad.id, set)
                      }
                    />
                    <span className="set-title">{set.name}</span>
                  </label>
                  <span className="set-meta">
                    {set.items.length} файлов
                  </span>
                </div>
                <div className="drawer-grid">
                  {set.items
                    .slice(0, pickerLimitBySet[set.id] ?? PICKER_CHUNK)
                    .map((item) => {
                      const realUrl = item.urls?.[selectedCabinetId] ?? item.url ?? "";
                    
                      return (
                        <label
                          key={item.id}
                          className={`drawer-item ${isItemSelected(item) ? "selected" : ""}`}
                        >
                          {item.type === "image" ? (
                            <img src={realUrl} className="drawer-thumb" alt={item.name} loading="lazy" />
                          ) : (
                            <img
                              src={item.thumbUrl || item.url}
                              className="drawer-thumb"
                              alt={item.name}
                              loading="lazy"
                            />
                          )}

                          <input
                            type="checkbox"
                            checked={isItemSelected(item)}
                            onChange={() => toggleMediaForAd(ad.id, item)}
                          />

                          <span className="drawer-item-name">{item.name}</span>
                        </label>
                      );
                    })}

                  {/* sentinel ДЛЯ ЭТОГО set */}
                  {(pickerLimitBySet[set.id] ?? PICKER_CHUNK) < set.items.length && (
                    <div
                      data-setid={set.id}
                      ref={registerPickerSentinel(set.id)}
                      style={{ height: 1 }}
                    />
                  )}
                </div>
              </div>
            ))}
          </div>
          <div className="drawer-footer">
            <button
              className="primary-button"
              onClick={closeVideoPicker}
            >
              Готово
            </button>
          </div>
        </div>
      </div>
    );
  };

  // === Render Sub1 Modal ===
  const renderSub1Modal = () => {
    if (!sub1ModalOpen) return null;

    const filtered = AVAILABLE_SUB1.filter(s =>
      s.toLowerCase().includes(sub1Search.toLowerCase())
    );

    const toggleSub1 = (s: string) => {
      setSub1Selected(prev =>
        prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s]
      );
    };

    const handleSave = () => {
      saveUserSub1(sub1Selected);
      setSub1ModalOpen(false);
    };

    return (
      <div className="popup-overlay" onClick={() => setSub1ModalOpen(false)}>
        <div 
          className="popup-window glass" 
          onClick={e => e.stopPropagation()} 
          style={{ width: 400, padding: 20 }}
        >
          <h3 style={{ margin: "0 0 16px" }}>Выберите sub1</h3>

          <input
            type="text"
            placeholder="Поиск..."
            value={sub1Search}
            onChange={e => setSub1Search(e.target.value)}
            style={{ width: "100%", marginBottom: 12 }}
          />

          <div style={{ display: "flex", flexDirection: "column", gap: 8, maxHeight: 300, overflowY: "auto" }}>
            {filtered.map(s => (
              <label 
                key={s} 
                style={{ 
                  display: "flex", 
                  alignItems: "center", 
                  gap: 10, 
                  cursor: "pointer",
                  padding: "8px 12px",
                  borderRadius: 8,
                  background: sub1Selected.includes(s) ? "var(--accent-soft)" : "transparent",
                  border: sub1Selected.includes(s) ? "1px solid var(--accent)" : "1px solid var(--border-soft)",
                }}
              >
                <input
                  type="checkbox"
                  checked={sub1Selected.includes(s)}
                  onChange={() => toggleSub1(s)}
                />
                <span style={{ fontWeight: sub1Selected.includes(s) ? 600 : 400 }}>{s}</span>
              </label>
            ))}
            {filtered.length === 0 && (
              <div className="hint">Ничего не найдено</div>
            )}
          </div>

          <div style={{ display: "flex", gap: 8, marginTop: 16, justifyContent: "flex-end" }}>
            <button className="outline-button" onClick={() => setSub1ModalOpen(false)}>
              Отмена
            </button>
            <button className="primary-button" onClick={handleSave}>
              Сохранить
            </button>
          </div>
        </div>
      </div>
    );
  };

  const renderImageCropModal = () => {
    if (!cropModalOpen || !currentCropTask || !cropPreviewUrl) return null;

    return (
      <div className="popup-overlay crop-overlay-root">
        <div className="crop-window glass">
          <div className="crop-header">
            <div>Обрезка изображения</div>
            <button className="icon-button" onClick={handleCropCancel}>
              ✕
            </button>
          </div>

          <div className="crop-formats">
            {IMAGE_FORMATS.map((f) => (
              <button
                key={f.id}
                className={`pill ${f.id === cropFormatId ? "active" : ""}`}
                onClick={() => handleChangeFormat(f.id)}
              >
                {f.label}
              </button>
            ))}
          </div>

          <div className="crop-stage">
            <div className="crop-inner" ref={cropInnerRef}>
              <img
                ref={cropImgRef}
                src={cropPreviewUrl!}
                alt="crop"
                onLoad={handleCropImageLoad}
              />

              {/* Контейнер по границам изображения */}
              <div
                style={{
                  position: "absolute",
                  left:  imgBox.left,
                  top:   imgBox.top,
                  width: imgBox.width,
                  height:imgBox.height,
                  pointerEvents: "none", // клики пропускаем, их ловит рамка
                }}
              >
                {(() => {
                  const img = cropImgRef.current;
                  const scaleX =
                    img && img.naturalWidth ? imgBox.width / img.naturalWidth : 1;
                  const scaleY =
                    img && img.naturalHeight ? imgBox.height / img.naturalHeight : 1;
                
                  return (
                    <div
                      className="crop-rect"
                      style={{
                        position: "absolute",
                        left:   cropRect.x * scaleX,
                        top:    cropRect.y * scaleY,
                        width:  cropRect.width  * scaleX,
                        height: cropRect.height * scaleY,
                        pointerEvents: "auto",
                      }}
                      onMouseDown={handleCropMouseDown}
                    >
                      <div className="crop-rect-border" />
                    </div>
                  );
                })()}
              </div>
            </div>
          </div>

          <div className="crop-footer">
            <button className="outline-button" onClick={handleCropCancel}>
              Отмена
            </button>
            <button className="primary-button" onClick={handleCropConfirm}>
              Обрезать и загрузить
            </button>
          </div>
        </div>
      </div>
    );
  };

  const renderMain = () => {
    if (view.type === "presetEditor") {
      return renderPresetEditor();
    }

    if (activeTab === "campaigns") return renderCampaignsHome();
    if (activeTab === "creatives") return renderCreativesPage();
    if (activeTab === "audiences") return renderAudiencesPage();
    if (activeTab === "textsets") return (
      <TextSetsPage
        apiBase={API_BASE}
        userId={userId || ""}
        cabinetId={selectedCabinetId}
        fetchSecured={fetchSecured}
      />
    );
    if (activeTab === "logo") return renderLogoPage();
    if (activeTab === "history") return renderHistoryPage();
    if (activeTab === "settings") return renderSettingsPage();
    if (activeTab === "misc") return (
      <TriggersPage
        apiBase={API_BASE}
        userId={userId || ""}
        cabinetId={selectedCabinetId}
        cabinets={cabinets}
        fetchSecured={fetchSecured}
      />
    );
    return null;
  };

  // ----------------- Mobile overlay -----------------
  if (isMobile) {
    return (
      <div className="mobile-overlay">
        <div className="mobile-card glass">
          <h1>Auto ADS</h1>
          <p>Откройте на ПК и растяните экран.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app-root">
      {renderHeader()}
      <div className={`app-body ${sidebarOpen ? "" : "sidebar-collapsed"}`}>
        {renderSidebar()}
        <main className="app-main">
          {renderBackBar()}
          {loading && (
            <div className="loading-overlay">
              <div className="loader" />
            </div>
          )}
          {(error || noCabinetsWarning) && (
            <div className="error-banner">
              {noCabinetsWarning ? "Кабинеты пока не добавлены" : error}
            </div>
          )}
          {renderMain()}
        </main>
      </div>
      {renderVideoPickerDrawer()}
      {renderImageCropModal()}
      {popup.open && (
        <div className="popup-overlay">
          <div className="popup-window glass">
            {popup.msg}
          </div>
        </div>
      )}
      {renderSub1Modal()}
      {pixelDialog.open && createPortal(
        <div className="popup-overlay" style={{ zIndex: 1000 }}>
          <div className="confirm-window glass">
            <div className="confirm-text">Добавить пиксель</div>

            <div className="form-field">
              <label>Домен</label>
              <input
                placeholder="example.com"
                value={pixelDialog.domain}
                onChange={(e) => setPixelDialog(p => ({ ...p, domain: e.target.value }))}
              />
            </div>

            <div className="form-field">
              <label>Пиксель сайта</label>
              <input
                placeholder="id"
                value={pixelDialog.pixel}
                onChange={(e) => setPixelDialog(p => ({ ...p, pixel: e.target.value }))}
              />
            </div>

            <div className="confirm-actions">
              <button className="outline-button" onClick={() => closePixelDialog(null)}>
                Отмена
              </button>
              <button
                className="primary-button"
                disabled={!pixelDialog.domain.trim() || !pixelDialog.pixel.trim()}
                onClick={() => closePixelDialog({
                  domain: pixelDialog.domain.trim(),
                  pixel: pixelDialog.pixel.trim()
                })}
              >
                Создать
              </button>
            </div>
          </div>
        </div>,
        document.body
      )}
      {confirmDialog.open && (
        <div className="popup-overlay" style={{ zIndex: 300 }}>
          <div className="confirm-window glass">
            <div className="confirm-text">{confirmDialog.msg}</div>
            <div className="confirm-actions">
              <button className="outline-button" onClick={() => closeConfirm(false)}>
                Отмена
              </button>
              <button className="primary-button" onClick={() => closeConfirm(true)}>
                Ок
              </button>
            </div>
          </div>
        </div>
      )}
      {contextMenu && contextMenu.visible && (
        <div
          className="context-menu"
          style={{
            position: "fixed",
            top: contextMenu.y,
            left: contextMenu.x,
            zIndex: 250,
          }}
        >
          <button
            className="context-menu-item"
            onClick={handleRehash}
          >
            Поменять хэш
          </button>
        </div>
      )}
    </div>
  );
};

export default App;
