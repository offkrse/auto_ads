import React, { useEffect, useMemo, useState } from "react";

// ===== Типы =====

type Theme = "light" | "dark";

type Audience = {
  id: number;
  name: string;
  created?: string;
  updated?: string;
  pass_condition?: number;
};

type CreativeItem = {
  id: string;
  name: string;
  type: "video" | "image";
  url: string;
};

type CreativeSet = {
  id: string;
  name: string;
  items: CreativeItem[];
};

type TextSet = {
  id: string;
  name: string;
  shortDescription: string;
  longDescription: string;
};

type Ad = {
  id: string;
  textSetId: string | "new";
  customTextSet: TextSet;
  selectedCreativeItemIds: string[];
};

type Group = {
  id: string;
  regions: string;
  gender: "any" | "male" | "female";
  age: string;
  interests: string;
  audienceIds: number[];
  ads: Ad[];
};

type TriggerType = "none" | "time";

type CompanySettings = {
  presetName: string;
  companyName: string;
  targetAction: string;
  trigger: TriggerType;
  time: string;
};

type Preset = {
  backendId?: string; // id пресета на бэке (preset_1 и т.д.)
  company: CompanySettings;
  groups: Group[];
};

type SelectedNode =
  | { type: "company" }
  | { type: "group"; groupId: string }
  | { type: "ad"; groupId: string; adId: string };

type MainTab = "campaigns" | "creatives" | "audiences";
type CampaignView = "list" | "presetEditor";

const randomId = () => Math.random().toString(36).slice(2, 10);

// ======= Основной компонент =======

const App: React.FC = () => {
  const [userId, setUserId] = useState<string | null>(null);
  const [theme, setTheme] = useState<Theme>("light");

  const [mainTab, setMainTab] = useState<MainTab>("campaigns");
  const [campaignView, setCampaignView] = useState<CampaignView>("list");

  const [currentPreset, setCurrentPreset] = useState<Preset | null>(null);
  const [savedPresets, setSavedPresets] = useState<Preset[]>([]);

  const [audiences, setAudiences] = useState<Audience[]>([]);
  const [creativeSets, setCreativeSets] = useState<CreativeSet[]>([]);

  const [showVideoPicker, setShowVideoPicker] = useState(false);
  const [videoPickerTarget, setVideoPickerTarget] = useState<{
    groupId: string;
    adId: string;
  } | null>(null);

  const [initLoading, setInitLoading] = useState(true);

  // === Получаем userId из Telegram WebApp ===
  useEffect(() => {
    const w = window as any;
    const tgUserId =
      w?.Telegram?.WebApp?.initDataUnsafe?.user?.id ??
      w?.Telegram?.WebApp?.initDataUnsafe?.user?.id ??
      null;

    if (tgUserId) {
      setUserId(String(tgUserId));
    } else {
      // режим разработки
      setUserId("dev_user");
    }
  }, []);

  // === Применяем тему к html ===
  useEffect(() => {
    if (theme === "dark") {
      document.documentElement.classList.add("dark");
    } else {
      document.documentElement.classList.remove("dark");
    }
  }, [theme]);

  // === Инициализация из бэка: настройки, креативы, пресеты ===
  useEffect(() => {
    if (!userId) return;

    (async () => {
      try {
        // настройки
        try {
          const res = await fetch(
            `api/auto_ads/settings/get?user_id=${encodeURIComponent(userId)}`
          );
          if (res.ok) {
            const data = await res.json();
            const t = data?.settings?.theme as Theme | undefined;
            if (t === "dark" || t === "light") {
              setTheme(t);
            }
          }
        } catch (e) {
          console.error("settings/get error", e);
        }

        // креативы
        try {
          const res = await fetch(
            `api/auto_ads/creatives/get?user_id=${encodeURIComponent(userId)}`
          );
          if (res.ok) {
            const data = await res.json();
            setCreativeSets((data?.creatives || []) as CreativeSet[]);
          }
        } catch (e) {
          console.error("creatives/get error", e);
        }

        // пресеты
        try {
          const res = await fetch(
            `api/auto_ads/preset/list?user_id=${encodeURIComponent(userId)}`
          );
          if (res.ok) {
            const data = await res.json();
            const presets: Preset[] = (data?.presets || []).map(
              (p: any) => ({
                backendId: p.preset_id,
                ...(p.data as Preset),
              })
            );
            setSavedPresets(presets);
          }
        } catch (e) {
          console.error("preset/list error", e);
        }
      } finally {
        setInitLoading(false);
      }
    })();
  }, [userId]);

  // === Сохранение настроек (только тема) ===
  const saveSettings = async (newTheme: Theme) => {
    if (!userId) return;
    try {
      await fetch("api/auto_ads/settings/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          settings: { theme: newTheme },
        }),
      });
    } catch (e) {
      console.error("settings/save error", e);
    }
  };

  const toggleTheme = () => {
    setTheme((prev) => {
      const next = prev === "light" ? "dark" : "light";
      saveSettings(next);
      return next;
    });
  };

  // === Создание нового пресета ===
  const createNewPreset = () => {
    const defaultGroupId = randomId();
    const defaultAdId = randomId();

    const preset: Preset = {
      backendId: undefined,
      company: {
        presetName: "",
        companyName: "",
        targetAction: "",
        trigger: "none",
        time: "",
      },
      groups: [
        {
          id: defaultGroupId,
          regions: "",
          gender: "any",
          age: "21-55",
          interests: "",
          audienceIds: [],
          ads: [
            {
              id: defaultAdId,
              textSetId: "new",
              customTextSet: {
                id: randomId(),
                name: "",
                shortDescription: "",
                longDescription: "",
              },
              selectedCreativeItemIds: [],
            },
          ],
        },
      ],
    };

    setCurrentPreset(preset);
    setCampaignView("presetEditor");
  };

  // === Открыть существующий пресет ===
  const openPreset = (preset: Preset) => {
    setCurrentPreset(preset);
    setCampaignView("presetEditor");
  };

  // === Сохранить текущий пресет ===
  const saveCurrentPreset = async () => {
    if (!userId || !currentPreset) return;

    try {
      const res = await fetch("api/auto_ads/preset/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          presetId: currentPreset.backendId || null,
          preset: {
            company: currentPreset.company,
            groups: currentPreset.groups,
          },
        }),
      });

      if (!res.ok) {
        alert("Ошибка сохранения пресета");
        return;
      }
      const data = await res.json();
      const presetId = data?.preset_id as string | undefined;

      if (presetId) {
        // обновляем текущий
        setCurrentPreset((prev) =>
          prev ? { ...prev, backendId: presetId } : prev
        );

        // обновляем список
        setSavedPresets((prev) => {
          const idx = prev.findIndex((p) => p.backendId === presetId);
          const updatedPreset: Preset = {
            ...currentPreset,
            backendId: presetId,
          };
          if (idx === -1) {
            return [...prev, updatedPreset];
          } else {
            const copy = [...prev];
            copy[idx] = updatedPreset;
            return copy;
          }
        });
      }

      alert("Пресет сохранён");
    } catch (e) {
      console.error("preset/save error", e);
      alert("Ошибка при сохранении пресета");
    }
  };

  // === Удалить пресет ===
  const deletePreset = async (preset: Preset) => {
    if (!userId || !preset.backendId) return;
    if (!confirm("Удалить этот пресет?")) return;

    try {
      const url = `api/auto_ads/preset/delete?user_id=${encodeURIComponent(
        userId
      )}&preset_id=${encodeURIComponent(preset.backendId)}`;
      const res = await fetch(url, { method: "DELETE" });
      if (!res.ok) {
        alert("Ошибка удаления");
        return;
      }
      setSavedPresets((prev) =>
        prev.filter((p) => p.backendId !== preset.backendId)
      );
      if (currentPreset?.backendId === preset.backendId) {
        setCurrentPreset(null);
        setCampaignView("list");
      }
    } catch (e) {
      console.error("preset/delete error", e);
      alert("Ошибка удаления пресета");
    }
  };

  // === Обновление пресета в редакторе ===
  const updateCurrentPreset = (updater: (prev: Preset) => Preset) => {
    setCurrentPreset((prev) => (prev ? updater(prev) : prev));
  };

  // === Сохранение креативов на бэке ===
  const saveCreativeSetsToServer = async () => {
    if (!userId) return;
    try {
      const res = await fetch("api/auto_ads/creatives/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          userId,
          creatives: creativeSets,
        }),
      });
      if (!res.ok) {
        alert("Ошибка сохранения креативов");
        return;
      }
      alert("Креативы сохранены");
    } catch (e) {
      console.error("creatives/save error", e);
      alert("Ошибка сохранения креативов");
    }
  };

  // === Выбор видео (панель справа) ===
  const applySelectedVideos = (selectedIds: string[]) => {
    if (!currentPreset || !videoPickerTarget) return;

    const { groupId, adId } = videoPickerTarget;

    updateCurrentPreset((prev) => ({
      ...prev,
      groups: prev.groups.map((g) =>
        g.id === groupId
          ? {
              ...g,
              ads: g.ads.map((a) =>
                a.id === adId
                  ? { ...a, selectedCreativeItemIds: selectedIds }
                  : a
              ),
            }
          : g
      ),
    }));

    setShowVideoPicker(false);
    setVideoPickerTarget(null);
  };

  // === Загрузка аудиторий с /api/v2/... ===
  useEffect(() => {
    if (mainTab !== "audiences") return;
    if (audiences.length > 0) return;

    (async () => {
      try {
        const res = await fetch(
          "/api/v2/remarketing/segments.json?limit=100"
        );
        if (!res.ok) return;
        const data = await res.json();
        setAudiences(data?.items || []);
      } catch (e) {
        console.error("audiences load error", e);
      }
    })();
  }, [mainTab, audiences.length]);

  if (!userId || initLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-slate-900 text-slate-100">
        Загрузка Auto ADS...
      </div>
    );
  }

  return (
    <div
      className={`min-h-screen flex flex-col bg-slate-50 text-slate-900 transition-colors duration-300 ${
        theme === "dark" ? "dark bg-slate-900 text-slate-100" : ""
      }`}
    >
      {/* HEADER */}
      <header className="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-950/80 backdrop-blur-md">
        <div className="flex items-center gap-4">
          {/* Переключатель темы */}
          <button
            onClick={toggleTheme}
            className="w-10 h-10 rounded-full border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-500 dark:text-slate-300 hover:shadow-md transition-shadow"
          >
            {theme === "light" ? "🌞" : "🌙"}
          </button>

          <span className="text-xl font-semibold tracking-tight">
            Auto ADS
          </span>
        </div>

        <div className="text-xs text-slate-400">
          userId: <span className="font-mono">{userId}</span>
        </div>
      </header>

      {/* BODY */}
      <div className="flex flex-1 overflow-hidden">
        {/* SIDEBAR */}
        <aside className="w-64 border-r border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-950/80 px-4 py-6 space-y-3">
          <SidebarTab
            active={mainTab === "campaigns"}
            label="Создание компаний"
            onClick={() => {
              setMainTab("campaigns");
              setCampaignView("list");
            }}
          />
          <SidebarTab
            active={mainTab === "creatives"}
            label="Креативы"
            onClick={() => setMainTab("creatives")}
          />
          <SidebarTab
            active={mainTab === "audiences"}
            label="Аудитории"
            onClick={() => setMainTab("audiences")}
          />
        </aside>

        {/* MAIN */}
        <main className="flex-1 p-6 overflow-auto">
          {mainTab === "campaigns" && campaignView === "list" && (
            <CampaignsListView
              onCreatePreset={createNewPreset}
              presets={savedPresets}
              onOpenPreset={openPreset}
              onDeletePreset={deletePreset}
            />
          )}

          {mainTab === "campaigns" &&
            campaignView === "presetEditor" &&
            currentPreset && (
              <PresetEditor
                preset={currentPreset}
                audiences={audiences}
                onBack={() => {
                  setCampaignView("list");
                  setCurrentPreset(null);
                }}
                onChange={updateCurrentPreset}
                onSave={saveCurrentPreset}
                creativeSets={creativeSets}
                onOpenVideoPicker={(groupId, adId) => {
                  setVideoPickerTarget({ groupId, adId });
                  setShowVideoPicker(true);
                }}
              />
            )}

          {mainTab === "creatives" && (
            <CreativesView
              creativeSets={creativeSets}
              setCreativeSets={setCreativeSets}
              onSave={saveCreativeSetsToServer}
              userId={userId}
            />
          )}

          {mainTab === "audiences" && (
            <AudiencesView audiences={audiences} />
          )}
        </main>

        {/* ПРАВАЯ ПАНЕЛЬ */}
        {showVideoPicker && (
          <VideoPickerPanel
            creativeSets={creativeSets}
            onClose={() => {
              setShowVideoPicker(false);
              setVideoPickerTarget(null);
            }}
            onApply={applySelectedVideos}
          />
        )}
      </div>
    </div>
  );
};

// ===== Компоненты =====

const SidebarTab: React.FC<{
  label: string;
  active: boolean;
  onClick: () => void;
}> = ({ label, active, onClick }) => (
  <button
    onClick={onClick}
    className={`w-full text-left px-3 py-2 rounded-xl text-sm font-medium transition-all ${
      active
        ? "bg-sky-500 text-white shadow-md"
        : "text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800"
    }`}
  >
    {label}
  </button>
);

const BackRow: React.FC<{ label?: string; onClick: () => void }> = ({
  label = "Назад",
  onClick,
}) => (
  <button
    onClick={onClick}
    className="inline-flex items-center gap-2 text-sm text-slate-500 hover:text-slate-800 dark:hover:text-slate-100 mb-4"
  >
    <span className="w-7 h-7 rounded-full border border-slate-300 dark:border-slate-600 flex items-center justify-center text-xs">
      ←
    </span>
    <span>{label}</span>
  </button>
);

// --- Список пресетов ---

const CampaignsListView: React.FC<{
  onCreatePreset: () => void;
  presets: Preset[];
  onOpenPreset: (p: Preset) => void;
  onDeletePreset: (p: Preset) => void;
}> = ({ onCreatePreset, presets, onOpenPreset, onDeletePreset }) => {
  return (
    <div className="max-w-3xl mx-auto space-y-6">
      <BackRow label="Назад" onClick={() => window.history.back()} />

      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-semibold">
          Создание пресетов компаний
        </h1>
        <button
          onClick={onCreatePreset}
          className="px-4 py-2 rounded-xl bg-sky-500 text-white text-sm font-medium shadow hover:bg-sky-600 transition-colors"
        >
          + Новый пресет
        </button>
      </div>

      {/* Карточка создать */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <button
          onClick={onCreatePreset}
          className="flex flex-col items-center justify-center border-2 border-dashed border-slate-300 dark:border-slate-700 rounded-2xl py-10 hover:border-sky-400 hover:bg-sky-50/40 dark:hover:bg-slate-800/60 transition-all"
        >
          <div className="w-12 h-12 rounded-full border border-sky-400 flex items-center justify-center text-sky-500 text-2xl mb-3 bg-white dark:bg-slate-900 shadow-sm">
            +
          </div>
          <span className="font-medium">Создать пресет</span>
          <span className="text-xs text-slate-500 mt-1">
            Компания → Группы → Объявления
          </span>
        </button>
      </div>

      {/* Список сохранённых пресетов */}
      <div className="mt-4">
        <h2 className="text-sm font-semibold mb-2">
          Сохранённые пресеты
        </h2>
        {presets.length === 0 && (
          <div className="text-sm text-slate-400">
            Пока нет сохранённых пресетов.
          </div>
        )}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {presets.map((p) => (
            <div
              key={p.backendId}
              className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-950/80 p-3 flex flex-col justify-between"
            >
              <div>
                <div className="text-xs text-slate-400 mb-1">
                  {p.backendId}
                </div>
                <div className="font-semibold text-sm">
                  {p.company.presetName || "Без названия"}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  Компания:{" "}
                  {p.company.companyName || "не указано"}
                </div>
              </div>
              <div className="mt-3 flex justify-between gap-2">
                <button
                  onClick={() => onOpenPreset(p)}
                  className="flex-1 px-3 py-1.5 rounded-xl text-xs bg-sky-500 text-white hover:bg-sky-600"
                >
                  Открыть
                </button>
                <button
                  onClick={() => onDeletePreset(p)}
                  className="px-3 py-1.5 rounded-xl text-xs bg-red-500/10 text-red-500 hover:bg-red-500/20"
                >
                  Удалить
                </button>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

// --- Редактор пресета ---

const PresetEditor: React.FC<{
  preset: Preset;
  audiences: Audience[];
  creativeSets: CreativeSet[];
  onBack: () => void;
  onChange: (updater: (prev: Preset) => Preset) => void;
  onSave: () => void;
  onOpenVideoPicker: (groupId: string, adId: string) => void;
}> = ({
  preset,
  audiences,
  onBack,
  onChange,
  onSave,
  creativeSets,
  onOpenVideoPicker,
}) => {
  const [selectedNode, setSelectedNode] = useState<SelectedNode>({
    type: "company",
  });

  const updateCompanyField = (
    field: keyof CompanySettings,
    value: string
  ) => {
    onChange((prev) => ({
      ...prev,
      company: { ...prev.company, [field]: value },
    }));
  };

  // --- Группы и объявления ---
  const addGroup = () => {
    const newGroupId = randomId();
    const newAdId = randomId();
    onChange((prev) => ({
      ...prev,
      groups: [
        ...prev.groups,
        {
          id: newGroupId,
          regions: "",
          gender: "any",
          age: "21-55",
          interests: "",
          audienceIds: [],
          ads: [
            {
              id: newAdId,
              textSetId: "new",
              customTextSet: {
                id: randomId(),
                name: "",
                shortDescription: "",
                longDescription: "",
              },
              selectedCreativeItemIds: [],
            },
          ],
        },
      ],
    }));
    setSelectedNode({ type: "group", groupId: newGroupId });
  };

  const copyGroup = (groupId: string) => {
    onChange((prev) => {
      const g = prev.groups.find((g) => g.id === groupId);
      if (!g) return prev;
      const newGroupId = randomId();
      const cloned: Group = {
        ...g,
        id: newGroupId,
        ads: g.ads.map((a) => ({
          ...a,
          id: randomId(),
          customTextSet: { ...a.customTextSet, id: randomId() },
        })),
      };
      return { ...prev, groups: [...prev.groups, cloned] };
    });
  };

  const deleteGroup = (groupId: string) => {
    onChange((prev) => ({
      ...prev,
      groups: prev.groups.filter((g) => g.id !== groupId),
    }));
    setSelectedNode({ type: "company" });
  };

  const updateGroupField = (
    groupId: string,
    field: keyof Group,
    value: any
  ) => {
    onChange((prev) => ({
      ...prev,
      groups: prev.groups.map((g) =>
        g.id === groupId ? { ...g, [field]: value } : g
      ),
    }));
  };

  const updateAd = (
    groupId: string,
    adId: string,
    updater: (prev: Ad) => Ad
  ) => {
    onChange((prev) => ({
      ...prev,
      groups: prev.groups.map((g) =>
        g.id === groupId
          ? {
              ...g,
              ads: g.ads.map((a) =>
                a.id === adId ? updater(a) : a
              ),
            }
          : g
      ),
    }));
  };

  const addAd = (groupId: string) => {
    const newAdId = randomId();
    onChange((prev) => ({
      ...prev,
      groups: prev.groups.map((g) =>
        g.id === groupId
          ? {
              ...g,
              ads: [
                ...g.ads,
                {
                  id: newAdId,
                  textSetId: "new",
                  customTextSet: {
                    id: randomId(),
                    name: "",
                    shortDescription: "",
                    longDescription: "",
                  },
                  selectedCreativeItemIds: [],
                },
              ],
            }
          : g
      ),
    }));
    setSelectedNode({ type: "ad", groupId, adId: newAdId });
  };

  const deleteAd = (groupId: string, adId: string) => {
    onChange((prev) => ({
      ...prev,
      groups: prev.groups.map((g) =>
        g.id === groupId
          ? { ...g, ads: g.ads.filter((a) => a.id !== adId) }
          : g
      ),
    }));
    setSelectedNode({ type: "group", groupId });
  };

  const selectedGroup =
    selectedNode.type === "group" || selectedNode.type === "ad"
      ? preset.groups.find((g) => g.id === selectedNode.groupId)
      : null;

  const selectedAd =
    selectedNode.type === "ad" && selectedGroup
      ? selectedGroup.ads.find((a) => a.id === selectedNode.adId)
      : null;

  return (
    <div className="h-full flex flex-col">
      <BackRow label="Назад к пресетам" onClick={onBack} />

      <div className="flex items-center justify-between mb-4">
        <div>
          <h2 className="text-xl font-semibold mb-1">
            Создание пресета
          </h2>
          <p className="text-xs text-slate-500 dark:text-slate-400">
            Компания → Группы → Объявления
          </p>
        </div>
        <button
          onClick={onSave}
          className="px-4 py-2 rounded-xl bg-sky-500 text-white text-sm font-medium shadow hover:bg-sky-600 transition-colors"
        >
          Сохранить пресет
        </button>
      </div>

      <div className="flex flex-1 gap-4 min-h-0">
        {/* Структура слева */}
        <div className="w-64 shrink-0 rounded-2xl bg-white/80 dark:bg-slate-950/80 border border-slate-200 dark:border-slate-800 p-4 overflow-auto">
          <div className="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-2">
            Структура
          </div>

          {/* Компания */}
          <button
            onClick={() => setSelectedNode({ type: "company" })}
            className={`flex items-center gap-2 w-full text-left text-sm px-2 py-2 rounded-xl mb-2 ${
              selectedNode.type === "company"
                ? "bg-sky-50 text-sky-600 dark:bg-sky-900/40 dark:text-sky-200"
                : "hover:bg-slate-100 dark:hover:bg-slate-800"
            }`}
          >
            <span>Компания</span>
          </button>

          {/* Группы / объявления */}
          <div className="space-y-2">
            {preset.groups.map((group, groupIndex) => (
              <div key={group.id} className="space-y-1">
                <div className="flex items-center justify-between">
                  <button
                    onClick={() =>
                      setSelectedNode({
                        type: "group",
                        groupId: group.id,
                      })
                    }
                    className={`flex items-center gap-2 text-sm px-2 py-1.5 rounded-xl flex-1 ${
                      selectedNode.type === "group" &&
                      selectedNode.groupId === group.id
                        ? "bg-sky-50 text-sky-600 dark:bg-sky-900/40 dark:text-sky-200"
                        : "hover:bg-slate-100 dark:hover:bg-slate-800"
                    }`}
                  >
                    <span className="text-xs text-slate-400">⤷</span>
                    <span>Группа {groupIndex + 1}</span>
                  </button>
                  <div className="flex items-center gap-1 ml-1">
                    <IconButton
                      title="Скопировать группу"
                      label="⧉"
                      onClick={() => copyGroup(group.id)}
                    />
                    <IconButton
                      title="Удалить группу"
                      label="🗑"
                      onClick={() => deleteGroup(group.id)}
                    />
                  </div>
                </div>

                <div className="pl-5 space-y-1">
                  {group.ads.map((ad, adIndex) => (
                    <div
                      key={ad.id}
                      className="flex items-center justify-between"
                    >
                      <button
                        onClick={() =>
                          setSelectedNode({
                            type: "ad",
                            groupId: group.id,
                            adId: ad.id,
                          })
                        }
                        className={`flex items-center gap-2 text-xs px-2 py-1 rounded-xl flex-1 ${
                          selectedNode.type === "ad" &&
                          selectedNode.groupId === group.id &&
                          selectedNode.adId === ad.id
                            ? "bg-sky-50 text-sky-600 dark:bg-sky-900/40 dark:text-sky-200"
                            : "hover:bg-slate-100 dark:hover:bg-slate-800"
                        }`}
                      >
                        <span className="text-xs text-slate-400">
                          ⤷
                        </span>
                        <span>Объявление {adIndex + 1}</span>
                      </button>
                      <IconButton
                        title="Удалить объявление"
                        label="🗑"
                        onClick={() => deleteAd(group.id, ad.id)}
                      />
                    </div>
                  ))}
                  <button
                    onClick={() => addAd(group.id)}
                    className="text-[11px] text-sky-500 hover:text-sky-600 mt-1"
                  >
                    + Добавить объявление
                  </button>
                </div>
              </div>
            ))}
          </div>

          <button
            onClick={addGroup}
            className="mt-3 text-xs text-sky-500 hover:text-sky-600"
          >
            + Добавить группу
          </button>
        </div>

        {/* Настройки справа */}
        <div className="flex-1 rounded-2xl bg-white/80 dark:bg-slate-950/80 border border-slate-200 dark:border-slate-800 p-4 overflow-auto">
          {selectedNode.type === "company" && (
            <CompanySettingsForm
              company={preset.company}
              onChange={updateCompanyField}
            />
          )}

          {selectedNode.type === "group" && selectedGroup && (
            <GroupSettingsForm
              group={selectedGroup}
              audiences={audiences}
              onChange={(field, value) =>
                updateGroupField(selectedGroup.id, field, value)
              }
            />
          )}

          {selectedNode.type === "ad" && selectedGroup && selectedAd && (
            <AdSettingsForm
              ad={selectedAd}
              creativeSets={creativeSets}
              onChange={(updater) =>
                updateAd(selectedGroup.id, selectedAd.id, updater)
              }
              onOpenVideoPicker={() =>
                onOpenVideoPicker(selectedGroup.id, selectedAd.id)
              }
            />
          )}
        </div>
      </div>
    </div>
  );
};

const IconButton: React.FC<{
  label: string;
  title?: string;
  onClick: () => void;
}> = ({ label, title, onClick }) => (
  <button
    type="button"
    title={title}
    onClick={onClick}
    className="w-6 h-6 rounded-lg flex items-center justify-center text-xs text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-800"
  >
    {label}
  </button>
);

// --- Формы настроек ---

const Field: React.FC<{
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
}> = ({ label, value, onChange, placeholder }) => (
  <div>
    <label className="block text-xs font-medium mb-1">{label}</label>
    <input
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      className="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-sky-400"
    />
  </div>
);

const SelectField: React.FC<{
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: { value: string; label: string }[];
}> = ({ label, value, onChange, options }) => (
  <div>
    <label className="block text-xs font-medium mb-1">{label}</label>
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-sky-400"
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  </div>
);

const CompanySettingsForm: React.FC<{
  company: CompanySettings;
  onChange: (field: keyof CompanySettings, value: string) => void;
}> = ({ company, onChange }) => (
  <div className="space-y-4">
    <h3 className="text-lg font-semibold mb-2">Компания</h3>

    <Field
      label="Название пресета"
      value={company.presetName}
      onChange={(v) => onChange("presetName", v)}
    />
    <Field
      label="Название компаний"
      value={company.companyName}
      onChange={(v) => onChange("companyName", v)}
    />
    <Field
      label="Целевое действие"
      value={company.targetAction}
      onChange={(v) => onChange("targetAction", v)}
      placeholder="Например: Лиды, Трафик, Конверсии"
    />

    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <SelectField
        label="Триггер"
        value={company.trigger}
        onChange={(v) => onChange("trigger", v as TriggerType)}
        options={[
          { value: "none", label: "Нет" },
          { value: "time", label: "Время" },
        ]}
      />
      {company.trigger === "time" && (
        <div>
          <label className="block text-xs font-medium mb-1">Время</label>
          <input
            type="time"
            value={company.time}
            onChange={(e) => onChange("time", e.target.value)}
            className="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-sky-400"
          />
        </div>
      )}
    </div>
  </div>
);

const GroupSettingsForm: React.FC<{
  group: Group;
  audiences: Audience[];
  onChange: (field: keyof Group, value: any) => void;
}> = ({ group, audiences, onChange }) => {
  const toggleAudience = (id: number) => {
    const exists = group.audienceIds.includes(id);
    if (exists) {
      onChange(
        "audienceIds",
        group.audienceIds.filter((x) => x !== id)
      );
    } else {
      onChange("audienceIds", [...group.audienceIds, id]);
    }
  };

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold mb-2">Группа</h3>

      <Field
        label="Регионы"
        value={group.regions}
        onChange={(v) => onChange("regions", v)}
        placeholder="Например: Москва, СПб"
      />

      <SelectField
        label="Пол"
        value={group.gender}
        onChange={(v) => onChange("gender", v as Group["gender"])}
        options={[
          { value: "any", label: "Любой" },
          { value: "male", label: "Мужской" },
          { value: "female", label: "Женский" },
        ]}
      />

      <Field
        label="Возраст"
        value={group.age}
        onChange={(v) => onChange("age", v)}
        placeholder="21-55"
      />

      <Field
        label="Интересы"
        value={group.interests}
        onChange={(v) => onChange("interests", v)}
        placeholder="Например: Авто, Недвижимость"
      />

      <div>
        <div className="flex items-center justify-between mb-1">
          <label className="block text-xs font-medium">Аудитории</label>
          <span className="text-[10px] text-slate-400">
            (в JSON сохраняются id)
          </span>
        </div>
        <div className="border rounded-xl border-slate-200 dark:border-slate-700 max-h-40 overflow-auto p-2 bg-slate-50 dark:bg-slate-900 text-xs space-y-1">
          {audiences.length === 0 && (
            <div className="text-slate-400">
              Аудитории не загружены или пусто
            </div>
          )}
          {audiences.map((a) => {
            const selected = group.audienceIds.includes(a.id);
            return (
              <button
                key={a.id}
                type="button"
                onClick={() => toggleAudience(a.id)}
                className={`w-full flex items-center justify-between px-2 py-1 rounded-lg ${
                  selected
                    ? "bg-sky-100 text-sky-700 dark:bg-sky-900/40 dark:text-sky-200"
                    : "hover:bg-slate-100 dark:hover:bg-slate-800"
                }`}
              >
                <span>{a.name}</span>
                <span className="text-[10px] text-slate-400">
                  id: {a.id}
                </span>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
};

const AdSettingsForm: React.FC<{
  ad: Ad;
  creativeSets: CreativeSet[];
  onChange: (updater: (prev: Ad) => Ad) => void;
  onOpenVideoPicker: () => void;
}> = ({ ad, creativeSets, onChange, onOpenVideoPicker }) => {
  const handleTextSetChange = (field: keyof TextSet, value: string) => {
    onChange((prev) => ({
      ...prev,
      customTextSet: { ...prev.customTextSet, [field]: value },
    }));
  };

  const availableTextSetOptions = [
    { value: "new", label: "Создать новый набор" },
  ];

  const selectedCreativeItems: CreativeItem[] = useMemo(() => {
    const items: CreativeItem[] = [];
    creativeSets.forEach((set) =>
      set.items.forEach((item) => {
        if (ad.selectedCreativeItemIds.includes(item.id)) {
          items.push(item);
        }
      })
    );
    return items;
  }, [creativeSets, ad.selectedCreativeItemIds]);

  return (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold mb-2">Объявление</h3>

      <SelectField
        label="Текстовый набор"
        value={ad.textSetId}
        onChange={(v) =>
          onChange((prev) => ({ ...prev, textSetId: v as any }))
        }
        options={availableTextSetOptions}
      />

      <Field
        label="Название текстового набора"
        value={ad.customTextSet.name}
        onChange={(v) => handleTextSetChange("name", v)}
      />
      <Field
        label="Короткое описание"
        value={ad.customTextSet.shortDescription}
        onChange={(v) => handleTextSetChange("shortDescription", v)}
      />
      <Field
        label="Длинное описание"
        value={ad.customTextSet.longDescription}
        onChange={(v) => handleTextSetChange("longDescription", v)}
      />

      <div className="space-y-2">
        <label className="block text-xs font-medium mb-1">
          Выбрать видео
        </label>
        <button
          type="button"
          onClick={onOpenVideoPicker}
          className="w-full flex items-center justify-between px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-sm hover:border-sky-400 hover:ring-1 hover:ring-sky-300 transition-all"
        >
          <span className="text-slate-500">Открыть список креативов</span>
          <span className="text-xs text-sky-500">
            {ad.selectedCreativeItemIds.length > 0
              ? `Выбрано: ${ad.selectedCreativeItemIds.length}`
              : "Не выбрано"}
          </span>
        </button>

        {selectedCreativeItems.length > 0 && (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2 mt-2">
            {selectedCreativeItems.map((item) => (
              <div
                key={item.id}
                className="border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden text-[10px]"
              >
                <div className="aspect-video bg-slate-200 dark:bg-slate-800">
                  <video
                    src={item.url}
                    className="w-full h-full object-cover"
                    muted
                  />
                </div>
                <div className="px-2 py-1 truncate">{item.name}</div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

// --- Креативы ---

const CreativesView: React.FC<{
  creativeSets: CreativeSet[];
  setCreativeSets: React.Dispatch<React.SetStateAction<CreativeSet[]>>;
  onSave: () => void;
  userId: string;
}> = ({ creativeSets, setCreativeSets, onSave, userId }) => {
  const [newSetName, setNewSetName] = useState("");

  const createSet = () => {
    if (!newSetName.trim()) return;
    const newSet: CreativeSet = {
      id: randomId(),
      name: newSetName.trim(),
      items: [],
    };
    setCreativeSets((prev) => [...prev, newSet]);
    setNewSetName("");
  };

  const uploadFiles = async (setId: string, files: FileList | null) => {
    if (!files || files.length === 0) return;

    const newItems: CreativeItem[] = [];

    for (const file of Array.from(files)) {
      const form = new FormData();
      form.append("file", file);

      try {
        const res = await fetch("api/auto_ads/upload", {
          method: "POST",
          body: form,
        });
        if (!res.ok) continue;
        const data = await res.json();
        const url = data.url as string;
        const ext = file.name.toLowerCase();
        const type: "video" | "image" =
          ext.endsWith(".mp4") ||
          ext.endsWith(".mov") ||
          ext.endsWith(".webm")
            ? "video"
            : "image";

        newItems.push({
          id: randomId(),
          name: file.name,
          type,
          url, // backend url (например /auto_ads/video/file.mp4)
        });
      } catch (e) {
        console.error("upload error", e);
      }
    }

    if (newItems.length > 0) {
      setCreativeSets((prev) =>
        prev.map((set) =>
          set.id === setId
            ? { ...set, items: [...set.items, ...newItems] }
            : set
        )
      );
    }
  };

  const deleteItem = (setId: string, itemId: string) => {
    setCreativeSets((prev) =>
      prev.map((set) =>
        set.id === setId
          ? {
              ...set,
              items: set.items.filter((i) => i.id !== itemId),
            }
          : set
      )
    );
  };

  return (
    <div className="max-w-5xl mx-auto">
      <BackRow label="Назад" onClick={() => window.history.back()} />

      <div className="flex items-center justify-between mb-2">
        <h1 className="text-2xl font-semibold">Креативы</h1>
        <button
          onClick={onSave}
          className="px-4 py-2 rounded-xl bg-sky-500 text-white text-sm font-medium shadow hover:bg-sky-600 transition-colors"
        >
          Сохранить изменения
        </button>
      </div>

      <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
        Создавайте наборы креативов, загружайте видео и картинки.
      </p>

      <div className="flex gap-2 mb-6">
        <input
          value={newSetName}
          onChange={(e) => setNewSetName(e.target.value)}
          placeholder="Название набора креативов"
          className="flex-1 px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-sky-400"
        />
        <button
          onClick={createSet}
          className="px-4 py-2 rounded-xl bg-sky-500 text-white text-sm font-medium shadow hover:bg-sky-600 transition-colors"
        >
          Создать набор
        </button>
      </div>

      <div className="space-y-4">
        {creativeSets.map((set) => (
          <div
            key={set.id}
            className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-950/80 p-4"
          >
            <div className="flex items-center justify-between mb-3">
              <div>
                <h2 className="text-sm font-semibold">{set.name}</h2>
                <p className="text-xs text-slate-400">
                  Элементов: {set.items.length}
                </p>
              </div>
            </div>

            <label className="block border-2 border-dashed border-slate-300 dark:border-slate-700 rounded-xl py-6 px-4 text-xs text-center text-slate-500 dark:text-slate-400 cursor-pointer hover:border-sky-400 hover:bg-sky-50/40 dark:hover:bg-slate-800/60 transition-colors">
              <input
                type="file"
                multiple
                className="hidden"
                onChange={(e) => uploadFiles(set.id, e.target.files)}
              />
              Перетащите файлы сюда или нажмите, чтобы выбрать
              <br />
              <span className="text-[10px] text-slate-400">
                Видео и изображения будут сохранены на сервере
              </span>
            </label>

            {set.items.length > 0 && (
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-4">
                {set.items.map((item) => (
                  <div
                    key={item.id}
                    className="relative border border-slate-200 dark:border-slate-700 rounded-xl overflow-hidden group"
                  >
                    <button
                      type="button"
                      onClick={() => deleteItem(set.id, item.id)}
                      className="absolute top-1.5 right-1.5 w-6 h-6 rounded-full bg-black/60 text-white text-xs flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity"
                    >
                      ✕
                    </button>
                    <div className="aspect-video bg-slate-200 dark:bg-slate-800">
                      {item.type === "video" ? (
                        <video
                          src={item.url}
                          className="w-full h-full object-cover"
                          muted
                        />
                      ) : (
                        <img
                          src={item.url}
                          alt={item.name}
                          className="w-full h-full object-cover"
                        />
                      )}
                    </div>
                    <div className="px-2 py-1 text-[10px] truncate">
                      {item.name}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}

        {creativeSets.length === 0 && (
          <div className="text-sm text-slate-400">
            Наборы креативов ещё не созданы.
          </div>
        )}
      </div>
    </div>
  );
};

// --- Аудитории ---

const AudiencesView: React.FC<{ audiences: Audience[] }> = ({
  audiences,
}) => (
  <div className="max-w-3xl mx-auto">
    <BackRow label="Назад" onClick={() => window.history.back()} />

    <h1 className="text-2xl font-semibold mb-2">Аудитории</h1>
    <p className="text-sm text-slate-500 dark:text-slate-400 mb-4">
      Список сегментов, полученных с бэка
      <br />
      <span className="text-xs text-slate-400">
        /api/v2/remarketing/segments.json?limit=100
      </span>
    </p>

    <div className="rounded-2xl border border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-950/80 overflow-hidden">
      <div className="grid grid-cols-[1fr_auto] gap-2 text-xs font-semibold px-4 py-2 border-b border-slate-200 dark:border-slate-800">
        <div>Название</div>
        <div className="text-right">ID</div>
      </div>
      <div className="max-h-[480px] overflow-auto text-xs">
        {audiences.map((a) => (
          <div
            key={a.id}
            className="grid grid-cols-[1fr_auto] gap-2 px-4 py-2 border-t border-slate-100 dark:border-slate-800"
          >
            <div className="truncate">{a.name}</div>
            <div className="text-right text-slate-400">#{a.id}</div>
          </div>
        ))}
        {audiences.length === 0 && (
          <div className="px-4 py-3 text-slate-400">
            Аудитории не загружены.
          </div>
        )}
      </div>
    </div>
  </div>
);

// --- Панель выбора видео ---

const VideoPickerPanel: React.FC<{
  creativeSets: CreativeSet[];
  onClose: () => void;
  onApply: (ids: string[]) => void;
}> = ({ creativeSets, onClose, onApply }) => {
  const [expandedSetIds, setExpandedSetIds] = useState<string[]>([]);
  const [selectedItemIds, setSelectedItemIds] = useState<string[]>([]);

  const toggleSetExpanded = (id: string) => {
    setExpandedSetIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    );
  };

  const toggleItem = (id: string) => {
    setSelectedItemIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    );
  };

  const toggleWholeSet = (set: CreativeSet) => {
    const allIds = set.items.map((i) => i.id);
    const allSelected = allIds.every((id) =>
      selectedItemIds.includes(id)
    );
    if (allSelected) {
      setSelectedItemIds((prev) =>
        prev.filter((id) => !allIds.includes(id))
      );
    } else {
      setSelectedItemIds((prev) =>
        Array.from(new Set([...prev, ...allIds]))
      );
    }
  };

  const apply = () => onApply(selectedItemIds);

  return (
    <div className="w-96 border-l border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-950 h-full flex flex-col shadow-xl">
      <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200 dark:border-slate-800">
        <span className="text-sm font-semibold">Выбор видео</span>
        <button
          onClick={onClose}
          className="text-xs text-slate-500 hover:text-slate-800 dark:hover:text-slate-100"
        >
          ✕
        </button>
      </div>

      <div className="flex-1 overflow-auto px-3 py-2 text-xs space-y-3">
        {creativeSets.map((set) => (
          <div
            key={set.id}
            className="border border-slate-200 dark:border-slate-800 rounded-xl overflow-hidden"
          >
            <div className="flex items-center justify-between px-3 py-2 bg-slate-50 dark:bg-slate-900">
              <button
                onClick={() => toggleSetExpanded(set.id)}
                className="flex-1 text-left flex items-center gap-2"
              >
                <span>
                  {expandedSetIds.includes(set.id) ? "▾" : "▸"}
                </span>
                <span>{set.name}</span>
              </button>
              <button
                onClick={() => toggleWholeSet(set)}
                className="text-[10px] text-sky-500"
              >
                {set.items.length > 0 &&
                set.items.every((i) => selectedItemIds.includes(i.id))
                  ? "Снять выбор"
                  : "Выбрать набор"}
              </button>
            </div>
            {expandedSetIds.includes(set.id) && (
              <div className="px-3 py-2 space-y-2">
                {set.items.map((item) => {
                  const selected = selectedItemIds.includes(item.id);
                  return (
                    <button
                      key={item.id}
                      onClick={() => toggleItem(item.id)}
                      className={`w-full flex items-center gap-2 rounded-lg border px-2 py-2 text-left ${
                        selected
                          ? "border-sky-400 bg-sky-50/70 dark:bg-sky-900/40"
                          : "border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-900"
                      }`}
                    >
                      <div className="w-10 h-10 rounded bg-slate-200 dark:bg-slate-800 overflow-hidden flex items-center justify-center text-[10px]">
                        {item.type === "video" ? (
                          <video
                            src={item.url}
                            className="w-full h-full object-cover"
                            muted
                          />
                        ) : (
                          <img
                            src={item.url}
                            alt={item.name}
                            className="w-full h-full object-cover"
                          />
                        )}
                      </div>
                      <div className="flex-1">
                        <div className="text-[11px] truncate">
                          {item.name}
                        </div>
                        <div className="text-[10px] text-slate-400">
                          {item.type === "video" ? "Видео" : "Картинка"}
                        </div>
                      </div>
                      <div className="text-xs">
                        {selected ? "✔" : "○"}
                      </div>
                    </button>
                  );
                })}
                {set.items.length === 0 && (
                  <div className="text-[11px] text-slate-400">
                    В наборе пока нет креативов
                  </div>
                )}
              </div>
            )}
          </div>
        ))}

        {creativeSets.length === 0 && (
          <div className="text-xs text-slate-400 px-1">
            Наборы креативов не созданы. Создайте их во вкладке
            &laquo;Креативы&raquo;.
          </div>
        )}
      </div>

      <div className="px-4 py-3 border-t border-slate-200 dark:border-slate-800 flex items-center justify-between">
        <span className="text-[11px] text-slate-500">
          Выбрано: {selectedItemIds.length}
        </span>
        <button
          onClick={apply}
          className="px-3 py-1.5 rounded-xl bg-sky-500 text-white text-xs font-medium hover:bg-sky-600 transition-colors disabled:opacity-50"
          disabled={selectedItemIds.length === 0}
        >
          Добавить в объявление
        </button>
      </div>
    </div>
  );
};

export default App;
