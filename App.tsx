import React, { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";

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
  targetAction: string;
  trigger: string;
  time?: string;
  url?: string;
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


const API_BASE = "/auto_ads/api";


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

type TabId = "campaigns" | "creatives" | "audiences" | "logo" | "history";

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

const formatHistoryHHmm = (val: unknown, addHoursShift = 4): string => {
  const ts = parseHistoryTS(val);
  if (!ts) return "—";
  const shifted = ts + addHoursShift * 3600_000;
  const d = new Date(shifted);
  // читаем ИМЕННО UTC, чтобы не было влияния локального пояса
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mm = String(d.getUTCMinutes()).padStart(2, "0");
  return `${hh}:${mm}`;
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
  const direction = useDropdownDirection(wrapRef as any, open, 260);
  const reqRef = React.useRef<AbortController | null>(null);
  const menuRef = React.useRef<HTMLDivElement | null>(null);
  const { onScrollCapture } = usePreserveScroll(menuRef, [
    open, q, loading, remote.length, vkAudiences.length
  ]);

  // Закрывать при потере фокуса
  React.useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, []);
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

  const toggleVk = async (id: string) => {
    const exists = selectedVkIds.includes(id);

    if (!exists) {
      const name = resolveName(id);

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
        vkIds:   [...selectedVkIds, id],
        vkNames: [...selectedVkNames, name],
        abstractNames: selectedAbstractNames
      });
    } else {
      const idx = selectedVkIds.indexOf(id);
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
            const name = selectedVkNames[i] || resolveName(id);
            return (
              <span
                key={id}
                className="pill active"
                onClick={(e)=>{ e.stopPropagation(); toggleVk(id); }}
              >
                {name} ✕
              </span>
            );
          })}
        </div>
      </div>

      {open && (
        <div
          ref={menuRef}
          className="aud-ms-menu glass"
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
              const active = selectedVkIds.includes(a.id);
              return (
                <button
                  type="button"
                  onMouseDown={(e) => e.preventDefault()}
                  key={a.id}
                  className={`pill ${active ? "active": ""}`}
                  onClick={() => toggleVk(a.id)}
                  style={{textAlign:"left"}}
                  title={a.created || ""}
                >
                  {/* В ИНТЕРФЕЙСЕ id не показываем */}
                  {a.name}
                </button>
              );
            })}
            {list.length === 0 && !loading && <div className="hint">Ничего не найдено</div>}
          </div>
        </div>
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
              className="icon-button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => setExpanded(prev => ({ ...prev, [node.id]: !opened }))}
              title={opened ? "Свернуть" : "Развернуть"}
            >
              {opened ? "▾" : "▸"}
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
          className="aud-ms-menu glass"
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

// ===== Дерево регионов с поиском и чипсами =====
type RegionItem = { id: number; name: string; parent_id?: number };
const regionNameCache: Record<number, string> = { 188: "Россия" };

const RegionsTreeSelect: React.FC<{
  selected: number[];
  onChange: (arr: number[]) => void;
}> = ({ selected, onChange }) => {
  const [items, setItems] = React.useState<RegionItem[]>([]);
  const [byParent, setByParent] = React.useState<Record<string, RegionItem[]>>({});
  const [nameById, setNameById] = React.useState<Record<number, string>>({ ...regionNameCache });
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

    if (wantMinus) {
      // переключаем конкретный минус
      next = next.filter(v => v !== plusId); // убираем возможный плюс того же региона
      next = next.includes(minusId)
        ? next.filter(v => v !== minusId)
        : [...next, minusId];

      // спец-правило: при наличии любых минусов Россия (188) ДОЛЖНА быть включена
      // т.е. разрешаем комбо [188] + (отрицательные регионы)
      next = next.filter(v => v !== -188); // -188 не бывает
      if (next.some(v => v < 0) && !next.includes(188)) {
        next = [188, ...next];
      }

      // не допускаем других плюсов, кроме 188
      next = next.filter(v => v < 0 || v === 188);

    } else {
      // режим включить
      next = next.filter(v => v !== minusId); // убираем возможный минус того же региона
      next = next.includes(plusId)
        ? next.filter(v => v !== plusId)
        : [...next, plusId];

      // в режиме включения 188 убираем, если выбраны какие-то плюсы
      if (next.some(v => v > 0 && v !== 188)) {
        next = next.filter(v => Math.abs(v) !== 188);
      }

      // и никаких минусов вместе с плюсами
      next = next.filter(v => v > 0);
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
              className="icon-button"
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => setExpanded(prev => ({ ...prev, [node.id]: !opened }))}
            >
              {opened ? "▾" : "▸"}
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
    const neg = id < 0;
    const pure = Math.abs(id);
    const nm = nameById[pure] ?? String(pure);
    return (
      <span key={`chip_r_${id}`} className="pill active" onClick={e => { e.stopPropagation(); onChange(selected.filter(v => v !== id)); }}>
        {neg ? "— " : ""}
        {nm} ✕
      </span>
    );
  });

  const hasPlusNow = selected.some(v => v > 0);
  const hasMinusNow = selected.some(v => v < 0);

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
          className="aud-ms-menu glass"
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
              {hasPlusNow && "Режим: включить"}
              {hasMinusNow && "Режим: исключить"}
              {!hasPlusNow && !hasMinusNow && "По умолчанию: 188"}
            </div>
          </div>

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
        setSelected(new Set());
      } catch (e: any) {
        setError(e.message || "Ошибка загрузки списков");
      } finally {
        setLoading(false);
      }
    },
    [userId, cabinetId]
  );

  React.useEffect(() => {
    // первая загрузка — последняя страница (offsetArg = null => -1 на бэке)
    loadPage(null);
  }, [loadPage]);

  const selectedIds = Array.from(selected);
  const canPrev = offset !== null && offset > 0;
  const canNext =
    offset !== null &&
    count > 0 &&
    offset + limit < count;

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

  return (
    <div className="users-lists-root">
      <div className="users-lists-top">
        <div className="section-tools">
          <span className="hint">
            Всего списков: {count}
            {offset !== null && ` • offset: ${offset}`}
          </span>
          <div style={{ marginLeft: "auto", display: "flex", gap: 8 }}>
            <button
              className="outline-button"
              disabled={!canPrev || loading}
              onClick={() =>
                offset !== null && loadPage(Math.max(0, offset - limit))
              }
            >
              &larr; Старее
            </button>
            <button
              className="outline-button"
              disabled={!canNext || loading}
              onClick={() =>
                offset !== null && loadPage(Math.min(Math.max(0, count - limit), offset + limit))
              }
            >
              Новее &rarr;
            </button>
          </div>
        </div>

        {error && <div className="error-banner">{error}</div>}

        <div className="users-lists-actions">
          <button
            className="primary-button"
            disabled={selectedIds.length === 0 || loading}
            onClick={() => setAction("merge")}
          >
            Объединить в аудиторию
          </button>
          <button
            className="outline-button"
            disabled={selectedIds.length === 0 || loading}
            onClick={() => createSegments("per_list")}
          >
            Создать по аудитории на список
          </button>
          <span className="hint">
            Выбрано списков: {selectedIds.length}
          </span>
        </div>
      </div>
      <div className="users-lists-table">
        <div className="users-lists-header">
          <div />
          <div>Название</div>
          <div>Статус</div>
          <div>Кол-во записей</div>
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
              {action === "merge"
                ? "Объединить выбранные списки в одну аудиторию"
                : "Создать отдельную аудиторию для каждого выбранного списка"}
            </div>
            <div className="form-field">
              <label>Название аудитории</label>
              <input
                value={segmentName}
                onChange={(e) => setSegmentName(e.target.value)}
              />
            </div>
            <div className="confirm-actions">
              <button className="outline-button" onClick={() => setAction(null)}>
                Отмена
              </button>
              <button
                className="primary-button"
                disabled={!segmentName.trim() || loading}
                onClick={handleRunAction}
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

// Нормализатор
function normalizePreset(raw: any): Preset {
  const company = {
    presetName: "",
    companyName: "",
    targetAction: "",
    trigger: "time",
    time: "",
    url: "",
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
  const [theme, setTheme] = useState<Theme>("dark");
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

  const [cabinets, setCabinets] = useState<Cabinet[]>([]);
  const [selectedCabinetId, setSelectedCabinetId] = useState<string>("all");

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
    if (stored === "light" || stored === "dark") {
      setTheme(stored);
      document.documentElement.setAttribute("data-theme", stored);
    }
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

    if (user?.id) {
      setUserId(String(user.id));  // <-- теперь будет реальный Telegram ID
    } else {
      console.warn("⚠️ Нет user.id в initDataUnsafe — demo_user");
      setUserId("demo_user");
    }
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
      } catch (e) {
        console.error(e);
        showPopup("Ошибка загрузки настроек");
      } finally {
        setLoading(false);
      }
    })();
  }, [userId]);

  // 2) Когда выбран кабинет — грузим всё остальное (с отменой запросов)
  useEffect(() => {
    if (!userId || !selectedCabinetId) return;

    const ac = new AbortController();
    const { signal } = ac;

    (async () => {
      setLoading(true);
      setError(null);
      const cabId = selectedCabinetId;

      try {
        // textsets (и для all тоже)
        try {
          const tJson = await apiJson(
            `${API_BASE}/textsets/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          setTextSets(Array.isArray(tJson.textsets) ? tJson.textsets : []);
        } catch (e) {
          if ((e as any).name !== "AbortError") {
            console.warn("textsets/get failed", e);
            setTextSets([]);
          }
        }

        // presets
        const pResp = await fetchSecured(
          `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
          { signal }
        );
        const pJson = await pResp.json();
        setPresets(Array.isArray(pJson.presets) ? pJson.presets : []);

        // queue statuses
        try {
          const map = await getQueueStatuses(userId, cabId);
          setQueueStatus(map);
        } catch (e) {
          console.warn("queue/status/get failed", e);
          // Фоллбек: если бекенд ещё не готов, считаем все active
          const fallback: Record<string,"active"|"deactive"> = {};
          (Array.isArray(pJson.presets) ? pJson.presets : []).forEach((p:any) => {
            fallback[p.preset_id] = "active";
          });
          setQueueStatus(fallback);
        }

        // creatives
        const cResp = await fetchSecured(
          `${API_BASE}/creatives/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
          { signal }
        );
        const cJson = await cResp.json();
        setCreativeSets(cJson.creatives || []);

        // audiences (VK локальные)
        const aResp = await fetchSecured(
          `${API_BASE}/audiences/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
          { signal }
        );
        const aJson = await aResp.json();
        setAudiences(aJson.audiences || []);
        // history
        try {
          const hJson = await apiJson(
            `${API_BASE}/history/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
            { signal }
          );
          setHistory(Array.isArray(hJson?.items) ? hJson.items : Array.isArray(hJson) ? hJson : []);
        } catch (e) {
          if ((e as any).name !== "AbortError") {
            console.warn("history/get failed", e);
            setHistory([]);
          }
        }
        // abstract — кабинетные
        const aaResp = await fetchSecured(
          `${API_BASE}/abstract_audiences/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`,
          { signal }
        );
        const aaJson = await aaResp.json();
        setAbstractAudiences(aaJson.audiences || []);
        try {
          const lgJson = await apiJson(
            `${API_BASE}/logo/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(cabId)}`
          );
          setLogo(lgJson.logo || null);
        } catch (e) {
          console.warn("logo/get failed", e);
          setLogo(null);
        }
      } catch (e: any) {
        if (e?.name !== "AbortError") {
          console.error(e);
          showPopup("Ошибка загрузки данных");
        }
      } finally {
        setLoading(false);
      }
    })();

    // Отмена всех запросов при смене кабинета/размонтаже
    return () => ac.abort();
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
        url: "",
      },
      groups: [
        {
          id: generateId(),
          groupName: "",
          budget: "600",
          regions: [188],
          gender: "male,female",
          age: "21-55",
          interests: [],
          audienceIds: [],
          audienceNames: [],
          abstractAudiences: [],
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
        url: "",
      },
      groups: [
        {
          id: generateId(),
          groupName: "",
          budget: "600",
          regions: [188],
          gender: "male,female",
          age: "21-55",
          interests: [],
          audienceIds: [],
          audienceNames: [],
          abstractAudiences: [],
          utm: "",
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
            const newSet: TextSet = {
              id: ad.textSetId!,
              name: ad.newTextSetName.trim() || "(без названия)",
              title: ad.title ?? "",
              shortDescription: ad.shortDescription,
              longDescription: ad.longDescription,
              ...(ad.advertiserInfo ? { advertiserInfo: ad.advertiserInfo } : {}),
              ...(ad.button ? { button: ad.button } : {}),
              ...(ad.logoId ? { logoId: ad.logoId } : {}),
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

  const renameCreativeSet = (id: string, name: string) => {
    const list = creativeSets.map((s) =>
      s.id === id ? { ...s, name } : s
    );
    setCreativeSets(list);
    saveCreatives(list);
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

      const resp = await fetchSecured(
        `${API_BASE}/upload?user_id=${encodeURIComponent(
          userId
        )}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`,
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
          `${API_BASE}/upload?user_id=${encodeURIComponent(
            userId
          )}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`,
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
        <button
          className="icon-button"
          onClick={toggleTheme}
          title="Переключить тему"
        >
          {theme === "light" ? "☀️" : "🌙"}
        </button>
      </div>
      <div className="header-center">
        <h1 className="app-title">Auto ADS</h1>
      </div>
      <div className="header-right">
        <div className="cabinet-select">
          <label>Кабинет</label>
          <select
            value={selectedCabinetId ?? ""}
            onChange={(e) => {
              const id = e.target.value;
              setSelectedCabinetId(id);
            
              // сохранить в backend
              fetchSecured(`${API_BASE}/settings/save`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  userId,
                  settings: { selected_cabinet_id: id }
                })
              }).catch(console.error);
            }}
          >
            {cabinets.map((cab) => (
              <option key={cab.id} value={cab.id}>
                {cab.name} {cab.id !== "all" && ` — id: ${cab.id}`}
              </option>
            ))}
          </select>
        </div>
      </div>
    </header>
  );

  const renderSidebar = () => (
    <aside className="sidebar glass">
      <div className="sidebar-tabs">
        <button
          className={`sidebar-tab ${
            activeTab === "campaigns" ? "active" : ""
          }`}
          onClick={() => {
            setActiveTab("campaigns");
            setView({ type: "home" });
          }}
        >
          Создание кампаний
        </button>
        <button
          className={`sidebar-tab ${
            activeTab === "creatives" ? "active" : ""
          }`}
          onClick={() => {
            setActiveTab("creatives");
            setView({ type: "home" });
          }}
        >
          Креативы
        </button>
        <button
          className={`sidebar-tab ${activeTab === "logo" ? "active" : ""}`}
          onClick={() => {
            setActiveTab("logo");
            setView({ type: "home" });
          }}
        >
          Логотип
        </button>
        <button
          className={`sidebar-tab ${
            activeTab === "audiences" ? "active" : ""
          }`}
          onClick={() => {
            setActiveTab("audiences");
            setView({ type: "home" });
          }}
        >
          Аудитории
        </button>
        <button
          className={`sidebar-tab ${activeTab === "history" ? "active" : ""}`}
          onClick={() => {
            setActiveTab("history");
            setView({ type: "home" });
          }}
        >
          История
        </button>
      </div>
    </aside>
  );

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
          ←
        </button>
        <span className="back-bar-title">
          {view.type === "presetEditor" && "Создание пресета"}
          {view.type === "creativeSetEditor" && "Набор креативов"}
        </span>
      </div>
    );
  };

  const renderCampaignsHome = () => (
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
          const groupsCount = Array.isArray(p?.data?.groups) ? p.data.groups.length : 0;
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
                {title}
                {viewMode === "list" && created && (
                  <span className="created-at">{created}</span>
                )}
              </div>
              
              <div className="preset-meta">
                <span>Групп: {groupsCount}</span>
                <span>Объявлений: {adsCount}</span>
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
                  🗐
                </button>
                <button
                  className="icon-button delete-button"
                  title="Удалить пресет"
                  onClick={(e) => { e.stopPropagation(); deletePreset(p.preset_id); }}
                >
                  🗑
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );

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
                    🗐
                  </button>
                  <button
                    className="icon-button"
                    title="Удалить группу"
                    onClick={() => deleteGroup(index)}
                  >
                    🗑️
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
      <div className="form-grid">
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
            onChange={(e) =>
              updateCompany({ targetAction: e.target.value })
            }
          >
            <option value="">Не выбрано</option>
            <option value="socialengagement">Сообщение в группу</option>
            <option value="site_conversions">На сайт</option>
            <option value="leadads">Лид</option>
          </select>
        </div>
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
        <div className="form-field">
          <label>Триггер</label>
          <select
            value={company.trigger}
            onChange={(e) =>
              updateCompany({ trigger: e.target.value })
            }
          >
            <option value="time">Время</option>
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
          <label>Бюджет</label>
          <input
            type="text"
            min={100}
            value={group.budget}
            onChange={(e) => updateGroup({ budget: e.target.value })}
          />
        </div>
        <div className="form-field">
          <label>UTM</label>
          <input
            type="text"
            value={group.utm}
            onChange={(e) => updateGroup({ utm: e.target.value })}
          />
        </div>
        <div className="form-field">
          <label>Регионы</label>
          <RegionsTreeSelect
            selected={group.regions}
            onChange={(arr) => {
              let next = [...arr];
            
              const hasMinus = next.some(v => v < 0);
              const hasPlusOtherThan188 = next.some(v => v > 0 && v !== 188);
            
              if (hasMinus) {
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
                    }}>🗑</button>
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
      <div className="form-grid">
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
          
        {/* Поля редактирования — ТОЛЬКО для «создать новый» */}
        {ad.isNewTextSet && (
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
                {CTA_OPTIONS.map(o => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>

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
    <div className="preset-editor glass">
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
                🗑
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
                {currentCreativeSet.items.map((item) => {
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
                        />
                      ) : (
                        <video
                          src={realUrl}
                          className="creative-thumb"
                          muted
                          loop
                          playsInline
                          preload="metadata"
                          {...(item.thumbUrl ? { poster: item.thumbUrl } : {})}
                        />
                      )}
                
                      <div className="creative-name">{item.name}</div>
                    
                      <button
                        className="icon-button delete-button"
                        onClick={() =>
                          deleteCreativeItem(currentCreativeSet.id, item.id)
                        }
                      >
                        🗑
                      </button>
                    </div>
                  );
                })}

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
        <div className="section-header">
          <div className="pill-select_aud">
            <button
              className={`pill ${audTab === "vk" ? "active" : ""}`}
              onClick={() => setAudTab("vk")}
            >
              Аудитории VK
            </button>
            <button
              className={`pill ${audTab === "lists" ? "active" : ""}`}
              onClick={() => setAudTab("lists")}
            >
              Списки
            </button>
            <button
              className={`pill ${audTab === "templates" ? "active" : ""}`}
              onClick={() => setAudTab("templates")}
            >
              Шаблоны аудиторий
            </button>
          </div>

          {audTab === "vk" && selectedCabinetId !== "all" && (
            <button className="primary-button" onClick={loadFromVK}>
              Обновить из VK
            </button>
          )}
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
                    🗑
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

    // свежие сверху
    const items = [...history].sort((a, b) => {
      const ta = parseHistoryTS(a?.date_time);
      const tb = parseHistoryTS(b?.date_time);
      return tb - ta;
    });

    return (
      <div className="content-section glass">
        <div className="section-header">
          <h2>История запусков</h2>
        </div>

        {items.length === 0 && (
          <div className="hint">Пока пусто</div>
        )}

        <div className="history-list">
          {items.map((it, idx) => {
            const ok = it?.status === "success";
            const timeHHmm = formatHistoryHHmm(it?.date_time);
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
                    <span className="time">{timeHHmm}</span>
                    <span className="sep">•</span>
                    <span className="trig">Триггер: {it?.trigger_time || "-"}</span>
                  </div>
                  <div className="drop-icon">{isOpen ? "▾" : "▸"}</div>
                </div>

                {isOpen && (
                  <div className="history-details">
                    {!ok && (
                      <div className="error-text">
                        {!ok && it?.text_error && it.text_error !== "null" && (
                          <div className="error-text">{String(it.text_error)}</div>
                        )}
                      </div>
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
          <div className="drawer-content">
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
                  {set.items.map((item) => {
                    const realUrl =
                      item.urls?.[selectedCabinetId] ??
                      item.url ?? "";

                    return (
                      <label
                        key={item.id}
                        className={`drawer-item ${isItemSelected(item) ? "selected" : ""}`}
                      >
                        {item.type === "image" ? (
                          <img src={realUrl} className="drawer-thumb" alt={item.name} />
                        ) : (
                          <video
                            src={realUrl}
                            className="drawer-thumb"
                            muted
                            loop
                            playsInline
                            preload="metadata"
                            {...(item.thumbUrl ? { poster: item.thumbUrl } : {})}
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
    if (activeTab === "logo") return renderLogoPage();
    if (activeTab === "history") return renderHistoryPage();
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
      <div className="app-body">
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
