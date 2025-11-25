import React, { useEffect, useMemo, useState } from "react";

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
  shortDescription: string;
  longDescription: string;
};

type CreativeItem = {
  id: string;
  url: string;
  name: string;
  type: "video" | "image";
  uploaded?: boolean;
  vkByCabinet?: Record<string, string>; // cabinet_id -> vk_id
  urls?: Record<string, string>; // cabinet_id -> local url
};

type CreativeSet = {
  id: string;
  name: string;
  items: CreativeItem[];
};

type Audience = {
  id: string;
  name: string;
};

type PresetCompany = {
  presetName: string;
  companyName: string;
  targetAction: string;
  trigger: string;
  time?: string;
};

type PresetGroup = {
  id: string;
  regions: string;
  gender: "male,female" | "male" | "female";
  age: string;
  interests: string;
  audienceIds: string[];
  budget: string;
  utm: string;
};

type PresetAd = {
  id: string;
  textSetId: string | null;
  newTextSetName: string;
  shortDescription: string;
  longDescription: string;
  videoIds: string[];
  creativeSetIds: string[];
  url: string;
};

type Preset = {
  company: PresetCompany;
  groups: PresetGroup[];
  ads: PresetAd[]; // один к одному по группам, но храним отдельно
};

const API_BASE = "/auto_ads/api";

type TabId = "campaigns" | "creatives" | "audiences";

type View =
  | { type: "home" }
  | { type: "presetEditor"; presetId?: string }
  | { type: "creativeSetEditor"; setId?: string };

const generateId = () => `id_${Math.random().toString(36).slice(2, 10)}`;

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
    setTimeout(() => setPopup({open: false, msg: ""}), 2500);
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

  const closeConfirm = (result: boolean) => {
    if (confirmDialog.resolve) confirmDialog.resolve(result);
    setConfirmDialog({ open: false, msg: "", resolve: undefined });
  };


  const [activeTab, setActiveTab] = useState<TabId>("campaigns");
  const [view, setView] = useState<View>({ type: "home" });

  const [cabinets, setCabinets] = useState<Cabinet[]>([]);
  const [selectedCabinetId, setSelectedCabinetId] = useState<string>("all");

  const [presets, setPresets] = useState<
    { preset_id: string; data: Preset }[]
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
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

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
  useEffect(() => {
    if (!userId) return;

    const loadAll = async () => {
      setLoading(true);
      setError(null);
      try {
        // settings (тут хранятся кабинеты)
        const sResp = await fetch(
          `${API_BASE}/settings/get?user_id=${encodeURIComponent(userId)}`
        );
        const sJson = await sResp.json();
        const settings = sJson.settings || {};

        // добавляем виртуальный кабинет "Все кабинеты"
        const cabinetsFromSettings: Cabinet[] = settings.cabinets ?? [];

        setCabinets(cabinetsFromSettings);
        if (cabinetsFromSettings.length <= 1) {
          setNoCabinetsWarning(true);
        } else {
          setNoCabinetsWarning(false);
        }
        // Если пользователь раньше выбирал кабинет — восстанавливаем
        if (settings.selected_cabinet_id) {
          setSelectedCabinetId(String(settings.selected_cabinet_id));
        } else {
          // иначе первый в списке ("Все кабинеты")
          setSelectedCabinetId(String(cabinetsFromSettings[0].id));
        }

        // presets
        const pResp = await fetch(
          `${API_BASE}/preset/list?user_id=${encodeURIComponent(
            userId
          )}&cabinet_id=${encodeURIComponent(selectedCabinetId || "all")}`
        );
        const pJson = await pResp.json();
        setPresets(pJson.presets || []);

        // creatives
        const cResp = await fetch(
          `${API_BASE}/creatives/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        const cJson = await cResp.json();
        setCreativeSets(cJson.creatives || []);

        // audiences
        const aResp = await fetch(
          `${API_BASE}/audiences/get?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`
        );
        const aJson = await aResp.json();
        setAudiences(aJson.audiences || []);
      } catch (e: any) {
        console.error(e);
        showPopup("Ошибка загрузки данных");
      } finally {
        setLoading(false);
      }
    };

    loadAll();
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
      },
      groups: [
        {
          id: generateId(),
          budget: "",
          regions: "",
          gender: "male,female",
          age: "21-55",
          interests: "",
          audienceIds: [],
          utm: ""
        },
      ],
      ads: [
        {
          id: generateId(),
          textSetId: null,
          newTextSetName: "",
          shortDescription: "",
          longDescription: "",
          videoIds: [],
          creativeSetIds: [],
          url: ""
        },
      ],
    };
    setPresetDraft(preset);
    setSelectedStructure({ type: "company" });
    setView({ type: "presetEditor", presetId: undefined });
  };

  const openPreset = (presetId: string, data: Preset) => {
    setPresetDraft(data);
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

      const resp = await fetch(`${API_BASE}/preset/save`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          cabinetId: selectedCabinetId,
          presetId,
          preset: presetDraft,
        }),
      });

      const json = await resp.json();
      if (!resp.ok) {
        throw new Error(json.detail || "Ошибка сохранения");
      }

      // обновляем список
      const pResp = await fetch(
        `${API_BASE}/preset/list?user_id=${encodeURIComponent(userId)}`
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

  const deletePreset = async (presetId: string) => {
    if (!userId) return;
    if (!(await askConfirm("Удалить этот пресет?"))) return;

    setSaving(true);
    setError(null);
    try {
      const resp = await fetch(
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

      const pResp = await fetch(
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

  // ----------------- Creatives helpers -----------------

  const currentCreativeSet = useMemo(
    () => creativeSets.find((s) => s.id === selectedCreativeSetId) || null,
    [creativeSets, selectedCreativeSetId]
  );

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
      await fetch(`${API_BASE}/creatives/save`, {
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

  const uploadCreativeFiles = async (files: FileList | null) => {
    if (!files || !currentCreativeSet) return;
    const newItems: CreativeItem[] = [];
    for (const file of Array.from(files)) {
      const formData = new FormData();
      formData.append("file", file);
      if (!userId) return;
      const resp = await fetch(
        `${API_BASE}/upload?user_id=${encodeURIComponent(userId)}&cabinet_id=${encodeURIComponent(selectedCabinetId)}`,
        { method: "POST", body: formData }
      );

      const json = await resp.json();
      if (!resp.ok || !json.results) {
          console.error("UPLOAD ERROR:", json);
          showPopup("Ошибка загрузки файла");
          continue;
      }
      // если один кабинет
      if (json.results.length === 1) {
          const r = json.results[0];
      
          newItems.push({
              id: r.vk_id,                                        // ID из VK ADS
              url: r.url,                                         // локальная ссылка
              name: file.name,
              type: file.type.startsWith("image") ? "image" : "video",
              uploaded: true,                                     // галочка
              vkByCabinet: { [r.cabinet_id]: r.vk_id }            // словарь cabinet_id → vk_id
          });
      } 
      // если выбран "all" — много кабинетов
      else {
          const vkByCabinet: Record<string, any> = {};
          const urls: Record<string, string> = {};
      
          json.results.forEach((r: any) => {
              vkByCabinet[r.cabinet_id] = r.vk_id;
              urls[r.cabinet_id] = r.url;
          });

          const firstUrl: string = json.results[0]?.url || "";

          newItems.push({
              id: generateId(),
              name: file.name,
              url: firstUrl,
              type: file.type.startsWith("image") ? "image" : "video",
              uploaded: true,
              vkByCabinet,
              urls,
          });
      }
    }

    const list = creativeSets.map((s) =>
      s.id === currentCreativeSet.id
        ? { ...s, items: [...s.items, ...newItems] }
        : s
    );
    setCreativeSets(list);
    saveCreatives(list);
  };

  const deleteCreativeItem = (setId: string, itemId: string) => {
    const list = creativeSets.map((s) =>
      s.id === setId
        ? {
            ...s,
            items: s.items.filter((it) => it.id !== itemId),
          }
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

  const toggleVideoForAd = (adId: string, item: CreativeItem) => {
    if (!presetDraft) return;
    const ads = presetDraft.ads.map((ad) => {
      if (ad.id !== adId) return ad;
      const already = ad.videoIds.includes(item.id);
      return {
        ...ad,
        videoIds: already
          ? ad.videoIds.filter((id) => id !== item.id)
          : [...ad.videoIds, item.id],
      };
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

  const getVideoById = (id: string) => {
    for (const set of creativeSets) {
      const item = set.items.find((it) => it.id === id);
      if (item) return item;
    }
    return null;
  };

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
              fetch(`${API_BASE}/settings/save`, {
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
      </div>
      <div className="preset-grid">
        <button className="preset-card add-card" onClick={startNewPreset}>
          <span className="plus-icon">+</span>
          <span>Новый пресет</span>
        </button>

        {presets.map((p) => (
          <div
            key={p.preset_id}
            className="preset-card"
            onClick={() => openPreset(p.preset_id, p.data)}
          >
            <div className="preset-name">
              {p.data.company.presetName || p.preset_id}
            </div>
            <div className="preset-meta">
              <span>Групп: {p.data.groups.length}</span>
              <span>Объявлений: {p.data.ads.length}</span>
            </div>
            <button
              className="icon-button delete-button"
              onClick={(e) => {
                e.stopPropagation();
                deletePreset(p.preset_id);
              }}
            >
              🗑
            </button>
          </div>
        ))}
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
      </div>
    );
  };

  const renderGroupSettings = () => {
    if (!presetDraft) return null;
    const index = selectedStructure.index ?? 0;
    const group = presetDraft.groups[index];

    const updateGroup = (patch: Partial<PresetGroup>) => {
      const groups = [...presetDraft.groups];
      groups[index] = { ...group, ...patch };
      setPresetDraft({ ...presetDraft, groups });
    };

    return (
      <div className="form-grid">
        <div className="form-field">
          <label>Бюджет</label>
          <input
            type="text"
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
          <input
            type="text"
            placeholder="..."
            value={group.regions}
            onChange={(e) => updateGroup({ regions: e.target.value })}
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
          <input
            type="text"
            value={group.interests}
            onChange={(e) =>
              updateGroup({ interests: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>Аудитории</label>
          <div className="pill-select">
            {audiences.length === 0 && (
              <span className="hint">
                Аудитории пока не созданы (вкладка «Аудитории»)
              </span>
            )}
            {audiences.map((a) => {
              const active = group.audienceIds.includes(a.id);
              return (
                <button
                  key={a.id}
                  type="button"
                  className={`pill ${active ? "active" : ""}`}
                  onClick={() => {
                    const ids = active
                      ? group.audienceIds.filter((id) => id !== a.id)
                      : [...group.audienceIds, a.id];
                    updateGroup({ audienceIds: ids });
                  }}
                >
                  {a.name}
                </button>
              );
            })}
          </div>
        </div>
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

    const currentTextSet: TextSet | null = ad.textSetId
      ? {
          id: ad.textSetId,
          name: ad.newTextSetName,
          shortDescription: ad.shortDescription,
          longDescription: ad.longDescription,
        }
      : null;

    const selectedVideos = ad.videoIds.map(getVideoById).filter(Boolean);

    const selectedCreativeSets = ad.creativeSetIds
      .map((id) => creativeSets.find((s) => s.id === id) || null)
      .filter(Boolean) as CreativeSet[];

    return (
      <div className="form-grid">
        <div className="form-field">
          <label>Текстовый набор</label>
          <select
            value={ad.textSetId ?? ""}
            onChange={(e) => {
              const val = e.target.value;
              if (val === "new") {
                updateAd({
                  textSetId: generateId(),
                  newTextSetName: "",
                  shortDescription: "",
                  longDescription: "",
                });
              } else if (!val) {
                updateAd({ textSetId: null });
              } else {
                // здесь можно в будущем подгружать сохранённые наборы
                updateAd({ textSetId: val });
              }
            }}
          >
            <option value="">Не выбран</option>
            <option value="new">Создать новый набор</option>
            {currentTextSet && ad.textSetId !== "new" && (
              <option value={currentTextSet.id}>
                {currentTextSet.name || "Набор"}
              </option>
            )}
          </select>
        </div>

        <div className="form-field">
          <label>Название текстового набора</label>
          <input
            type="text"
            value={ad.newTextSetName}
            onChange={(e) =>
              updateAd({ newTextSetName: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>Короткое описание</label>
          <textarea
            rows={2}
            value={ad.shortDescription}
            onChange={(e) =>
              updateAd({ shortDescription: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>Длинное описание</label>
          <textarea
            rows={4}
            value={ad.longDescription}
            onChange={(e) =>
              updateAd({ longDescription: e.target.value })
            }
          />
        </div>
        <div className="form-field">
          <label>URL</label>
          <input
            type="text"
            value={ad.url}
            onChange={(e) => updateAd({ url: e.target.value })}
          />
        </div>
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
                {currentCreativeSet.items.map((item) => {
                  const realUrl =
                    item.urls?.[selectedCabinetId] ??
                    item.url ?? "";
                                
                  return (
                    <div key={item.id} className="creative-card">
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

                {currentCreativeSet.items.length === 0 && (
                  <div className="hint">
                    Загрузите видео или картинки
                  </div>
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

  const renderAudiencesPage = () => (
    <div className="content-section glass">
      <div className="section-header">
        <h2>Аудитории</h2>
      </div>
      <div className="hint">
        Здесь ничего нет
      </div>
    </div>
  );

  const renderVideoPickerDrawer = () => {
    if (!videoPicker.open || !videoPicker.adId || !presetDraft) return null;
    const ad = getAdById(videoPicker.adId);
    if (!ad) return null;

    const isVideoSelected = (id: string) => ad.videoIds.includes(id);
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
                        className={`drawer-item ${isVideoSelected(item.id) ? "selected" : ""}`}
                      >
                        {item.type === "image" ? (
                          <img src={realUrl} className="drawer-thumb" alt={item.name} />
                        ) : (
                          <video src={realUrl} className="drawer-thumb" muted loop />
                        )}

                        <input
                          type="checkbox"
                          checked={isVideoSelected(item.id)}
                          onChange={() => toggleVideoForAd(ad.id, item)}
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

  const renderMain = () => {
    if (view.type === "presetEditor") {
      return renderPresetEditor();
    }

    if (activeTab === "campaigns") return renderCampaignsHome();
    if (activeTab === "creatives") return renderCreativesPage();
    if (activeTab === "audiences") return renderAudiencesPage();
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
    </div>
  );
};

export default App;
