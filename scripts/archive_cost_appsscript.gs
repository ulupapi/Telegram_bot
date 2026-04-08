    const ARCHIVE_COST_CONFIG = {
      targetSheetName: 'Себестоимость архив',
      sourceSheetPattern: /^\d{4}\s.+\sChecked$/i
    };

    const ARCHIVE_COST_DEFAULTS = {
      figureBase: 7.370876855,
      gluePerFigure: 0.3263157895,
      bricksPerFigure: 10,
      stickerPerTorso: 0.1192857143,
      torsoInkPerTorso: 0.01831168831,
      printPerCustomization: 50,
      filmPerFace: 0.1051785714,
      faceInkPerFace: 0.004577922078,
      backgroundBase: 200,
      photoPaper: 5.98,
      backgroundInk: 1.538181818,
      stretchWrap: 10.2,
      bubbleWrap: 23.25,
      payrollAssemblerRate: 0.1,
      payrollManagersRate: 0.15,
      payrollSmmRate: 0.1,
      taxRate: 0.06,
      frameCostBySize: {
        '14x14': 130.3909792,
        '17x21': 166.492696,
        '17x22': 166.492696,
        '22x22': 214.282552,
        '22x31': 295.5191328
      }
    };

    function onOpen() {
      SpreadsheetApp.getUi()
        .createMenu('Архив: Себестоимость')
        .addItem('Обновить свод', 'updateArchiveCostSheet')
        .addToUi();
    }

    function updateArchiveCostSheet() {
      const ss = SpreadsheetApp.getActive();
      const sourceSheets = ss
        .getSheets()
        .map((sheet) => sheet.getName())
        .filter((name) => isArchiveMonthSheetName_(name))
        .sort();

      if (!sourceSheets.length) {
        throw new Error('Не найдены листы по шаблону "YYYY <месяц> Checked/Cheсked".');
      }

      const target = getOrCreateSheet_(ss, ARCHIVE_COST_CONFIG.targetSheetName);
      ensureGridSize_(target, 1000, 31);

      const constants = readConstants_(target);
      const orders = [];

      sourceSheets.forEach((sheetName) => {
        const sheet = ss.getSheetByName(sheetName);
        const rowCount = Math.max(sheet.getLastRow() - 2, 0);
        if (!rowCount) return;
        const values = sheet.getRange(3, 1, rowCount, 43).getValues();
        values.forEach((row) => {
          const order = extractOrder_(row, sheetName);
          if (order) orders.push(order);
        });
      });

      orders.sort((a, b) => {
        if (a.requestDateSortKey !== b.requestDateSortKey) {
          return a.requestDateSortKey - b.requestDateSortKey;
        }
        return String(a.orderNumber).localeCompare(String(b.orderNumber), 'ru');
      });

      const bodyRows = orders.map((order) => toRawOutputRow_(order));
      const dataRowsCount = Math.max(bodyRows.length, 1);
      const totalRowsForFormat = dataRowsCount + 2;

      ensureGridSize_(target, totalRowsForFormat + 20, 31);
      target.getRange(1, 1, target.getMaxRows(), 25).clearContent();

      target.getRange(1, 1, 1, 25).setValues([buildHeader_()]);
      writeSummaryRow_(target);

      if (bodyRows.length > 0) {
        target.getRange(3, 1, bodyRows.length, 25).setValues(bodyRows);
      }

      installArrayFormulas_(target);
      writeConstants_(target, constants);
      applyFormats_(target, totalRowsForFormat);
    }

    function getOrCreateSheet_(ss, sheetName) {
      const existing = ss.getSheetByName(sheetName);
      if (existing) return existing;
      return ss.insertSheet(sheetName);
    }

    function ensureGridSize_(sheet, minRows, minCols) {
      const currentRows = sheet.getMaxRows();
      const currentCols = sheet.getMaxColumns();
      if (currentRows < minRows) {
        sheet.insertRowsAfter(currentRows, minRows - currentRows);
      }
      if (currentCols < minCols) {
        sheet.insertColumnsAfter(currentCols, minCols - currentCols);
      }
    }

    function normalizeSheetNameForMatch_(name) {
      const confusableMap = {
        а: 'a',
        в: 'b',
        с: 'c',
        е: 'e',
        к: 'k',
        м: 'm',
        н: 'h',
        о: 'o',
        р: 'p',
        т: 't',
        у: 'y',
        х: 'x'
      };

      return String(name || '')
        .trim()
        .toLowerCase()
        .replace(/[авсекмнортух]/g, (ch) => confusableMap[ch] || ch)
        .replace(/\s+/g, ' ');
    }

    function isArchiveMonthSheetName_(name) {
      const normalized = normalizeSheetNameForMatch_(name);
      return /^\d{4}\s.+\schecked$/.test(normalized);
    }

    function extractOrder_(row, sourceSheetName) {
      const orderNumber = String(row[0] || '').trim();
      if (!orderNumber) return null;

      return {
        orderNumber: orderNumber,
        orderPrice: parseNumber_(row[4]),
        size: normalizeSizeDisplay_(row[18]),
        figureCount: parseCount_(row[21]),
        torsoCustomizationCount: parseCount_(row[35]),
        faceCustomizationCount: parseCount_(row[36]),
        customBackgroundRaw: String(row[37] || '').trim(),
        accessoriesCount: parseCount_(row[38]),
        petsCount: parseCount_(row[39]),
        giftWrapRaw: String(row[40] || '').trim(),
        giftCardRaw: String(row[41] || '').trim(),
        requestDateRaw: row[3],
        requestDateSortKey: buildDateSortKey_(row[3]),
        sourceSheetName: sourceSheetName
      };
    }

    function toRawOutputRow_(order) {
      return [
        order.orderNumber,
        order.orderPrice,
        order.size,
        order.figureCount,
        order.torsoCustomizationCount,
        order.faceCustomizationCount,
        toYesNo_(order.customBackgroundRaw),
        order.accessoriesCount,
        order.petsCount,
        toYesNo_(order.giftWrapRaw),
        toYesNo_(order.giftCardRaw),
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        '',
        order.sourceSheetName,
        normalizeRequestDateForCell_(order.requestDateRaw)
      ];
    }

    function buildHeader_() {
      return [
        'Номер заказа',
        'Стоимость заказа',
        'Размер',
        'Кол-во фигурок',
        'Кастомизация торсиков',
        'Кастомизация лиц',
        'Кастомный фон',
        'Кол-во аксессуаров',
        'Кол-во питомцев',
        'Подарочная упаковка',
        'Подарочная открытка',
        'Себестоимость рамки',
        'Себестоимость фигурок',
        'Себестоимость торсиков',
        'Себестоимость лиц',
        'Себестоимость фона',
        'Упаковка',
        'ФОТ',
        'Себестоимость без налога',
        'Налоги',
        'Себестоимость с налогами',
        'Валовая прибыль',
        'Маржа в %',
        'Источник лист',
        'Дата обращения'
      ];
    }

    function writeSummaryRow_(sheet) {
      sheet.getRange('A2').setValue('ИТОГО');
      sheet.getRange('B2').setFormula('=SUM(B3:B)');
      sheet.getRange('D2').setFormula('=COUNTA(A3:A)');
      sheet.getRange('U2').setFormula('=SUM(U3:U)');
      sheet.getRange('V2').setFormula('=SUM(V3:V)');
      sheet.getRange('W2').setFormula('=IFERROR(AVERAGEIF(A3:A;"<>";W3:W);0)');
    }

    function installArrayFormulas_(sheet) {
      const formulas = {
        L3: '=ARRAYFORMULA(IF(A3:A="";;IFERROR(VLOOKUP(C3:C;$AD$2:$AE$20;2;FALSE);0)))',
        M3: '=ARRAYFORMULA(IF(A3:A="";;N(D3:D)*($AB$2+$AB$3+$AB$4)))',
        N3: '=ARRAYFORMULA(IF(A3:A="";;N(E3:E)*($AB$5+$AB$6+$AB$7)))',
        O3: '=ARRAYFORMULA(IF(A3:A="";;N(F3:F)*($AB$8+$AB$9+$AB$7)))',
        P3: '=ARRAYFORMULA(IF(A3:A="";;IF(REGEXMATCH(LOWER(TO_TEXT(G3:G));"да|yes|true|1");$AB$10+$AB$11+$AB$12;0)))',
        Q3: '=ARRAYFORMULA(IF(A3:A="";;$AB$13+$AB$14))',
        R3: '=ARRAYFORMULA(IF(A3:A="";;N(B3:B)*($AB$15+$AB$16+$AB$17)))',
        S3: '=ARRAYFORMULA(IF(A3:A="";;N(L3:L)+N(M3:M)+N(N3:N)+N(O3:O)+N(P3:P)+N(Q3:Q)+N(R3:R)))',
        T3: '=ARRAYFORMULA(IF(A3:A="";;N(B3:B)*$AB$18))',
        U3: '=ARRAYFORMULA(IF(A3:A="";;N(S3:S)+N(T3:T)))',
        V3: '=ARRAYFORMULA(IF(A3:A="";;N(B3:B)-N(U3:U)))',
        W3: '=ARRAYFORMULA(IF(A3:A="";;IFERROR(N(V3:V)/N(B3:B);0)))'
      };

      Object.keys(formulas).forEach((cell) => {
        sheet.getRange(cell).setFormula(formulas[cell]);
      });
    }

    function readConstants_(sheet) {
      const constants = JSON.parse(JSON.stringify(ARCHIVE_COST_DEFAULTS));
      const base = sheet.getRange('AA2:AB40').getValues();
      const frame = sheet.getRange('AD2:AE20').getValues();

      const map = {};
      base.forEach((row) => {
        if (!row[0]) return;
        map[String(row[0]).trim().toLowerCase()] = parseNumber_(row[1]);
      });

      const labelToKey = {
        'себестоимость 1 фигурки': 'figureBase',
        'суперклей на 1 фигурку': 'gluePerFigure',
        'кубики на 1 фигурку': 'bricksPerFigure',
        'самоклейка на 1 торсик': 'stickerPerTorso',
        'чернила на 1 торсик': 'torsoInkPerTorso',
        'принт на 1 кастомизацию': 'printPerCustomization',
        'пленка на 1 лицо': 'filmPerFace',
        'чернила на 1 лицо': 'faceInkPerFace',
        'фон': 'backgroundBase',
        'фотобумага': 'photoPaper',
        'чернила на фон': 'backgroundInk',
        'стретч-пленка': 'stretchWrap',
        'пузырчатая пленка': 'bubbleWrap',
        'фот сборщика': 'payrollAssemblerRate',
        'фот менеджеров': 'payrollManagersRate',
        'фот smm': 'payrollSmmRate',
        'налоги': 'taxRate'
      };

      Object.keys(labelToKey).forEach((label) => {
        if (map[label] || map[label] === 0) constants[labelToKey[label]] = map[label];
      });

      frame.forEach((row) => {
        const size = normalizeSizeKey_(row[0]);
        if (!size) return;
        const value = parseNumber_(row[1]);
        if (value > 0) constants.frameCostBySize[size] = value;
      });

      return constants;
    }

    function writeConstants_(sheet, c) {
      const left = [
        ['Константа', 'Значение'],
        ['Себестоимость 1 фигурки', c.figureBase],
        ['Суперклей на 1 фигурку', c.gluePerFigure],
        ['Кубики на 1 фигурку', c.bricksPerFigure],
        ['Самоклейка на 1 торсик', c.stickerPerTorso],
        ['Чернила на 1 торсик', c.torsoInkPerTorso],
        ['Принт на 1 кастомизацию', c.printPerCustomization],
        ['Пленка на 1 лицо', c.filmPerFace],
        ['Чернила на 1 лицо', c.faceInkPerFace],
        ['Фон', c.backgroundBase],
        ['Фотобумага', c.photoPaper],
        ['Чернила на фон', c.backgroundInk],
        ['Стретч-пленка', c.stretchWrap],
        ['Пузырчатая пленка', c.bubbleWrap],
        ['ФОТ сборщика', c.payrollAssemblerRate],
        ['ФОТ менеджеров', c.payrollManagersRate],
        ['ФОТ SMM', c.payrollSmmRate],
        ['Налоги', c.taxRate]
      ];
      const right = [
        ['Размер', 'Себестоимость рамки'],
        ['14 x 14', c.frameCostBySize['14x14'] || 0],
        ['17 x 21', c.frameCostBySize['17x21'] || 0],
        ['17 x 22', c.frameCostBySize['17x22'] || c.frameCostBySize['17x21'] || 0],
        ['22 x 22', c.frameCostBySize['22x22'] || 0],
        ['22 x 31', c.frameCostBySize['22x31'] || 0]
      ];
      sheet.getRange(1, 27, left.length, 2).setValues(left);
      sheet.getRange(1, 30, right.length, 2).setValues(right);
    }

    function applyFormats_(sheet, rowCount) {
      sheet.setFrozenRows(2);
      sheet.getRange(1, 1, 2, 25).setFontWeight('bold');
      sheet.getRange(2, 2, Math.max(rowCount - 1, 1), 1).setNumberFormat('#,##0.00');
      sheet.getRange(2, 12, Math.max(rowCount - 1, 1), 11).setNumberFormat('#,##0.00');
      sheet.getRange(2, 23, Math.max(rowCount - 1, 1), 1).setNumberFormat('0.00%');
      if (rowCount > 2) {
        sheet.getRange(3, 25, rowCount - 2, 1).setNumberFormat('dd.mm.yyyy');
      }
      const existingFilter = sheet.getFilter();
      if (existingFilter) existingFilter.remove();
      sheet.getRange(1, 1, rowCount, 25).createFilter();
      sheet.autoResizeColumns(1, 25);
    }

    function normalizeSizeKey_(value) {
      return String(value || '')
        .toLowerCase()
        .replace(/[хx×]/g, 'x')
        .replace(/\s+/g, '');
    }

    function normalizeSizeDisplay_(value) {
      const key = normalizeSizeKey_(value);
      const map = {
        '14x14': '14 x 14',
        '17x21': '17 x 21',
        '17x22': '17 x 22',
        '22x22': '22 x 22',
        '22x31': '22 x 31'
      };
      return map[key] || String(value || '').trim();
    }

    function buildDateSortKey_(value) {
      const normalized = normalizeRequestDateForCell_(value);

      if (normalized instanceof Date && !Number.isNaN(normalized.getTime())) {
        return normalized.getTime();
      }

      if (typeof normalized === 'number' && Number.isFinite(normalized)) {
        // Числа из Google Sheets обычно даты-сериалы ~45000.
        if (normalized > 20000 && normalized < 100000) return normalized;
        // unix ms timestamp
        if (normalized > 1e11 && normalized < 1e14) return normalized;
      }

      return Number.POSITIVE_INFINITY;
    }

    function normalizeRequestDateForCell_(value) {
      if (value instanceof Date && !Number.isNaN(value.getTime())) return value;
      if (typeof value === 'number' && Number.isFinite(value)) {
        // Дата-сериал Google Sheets
        if (value > 20000 && value < 100000) return value;
        // unix ms timestamp
        if (value > 1e11 && value < 1e14) {
          const date = new Date(value);
          return Number.isNaN(date.getTime()) ? '' : date;
        }
        return '';
      }

      const text = String(value || '').trim();
      if (!text) return '';

      const parsedDate = parseDateText_(text);
      if (parsedDate) return parsedDate;

      return '';
    }

    function parseDateText_(text) {
      const value = String(text || '').trim();
      if (!value) return null;

      // 12.02.2025 или 12.02.2025 00:00:03
      let m = value.match(
        /^(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
      );
      if (m) {
        const day = Number(m[1]);
        const month = Number(m[2]) - 1;
        const year = Number(m[3]);
        const h = Number(m[4] || 0);
        const min = Number(m[5] || 0);
        const sec = Number(m[6] || 0);
        const d = new Date(year, month, day, h, min, sec);
        return Number.isNaN(d.getTime()) ? null : d;
      }

      // 2025-02-12 или 2025-02-12 00:00:03
      m = value.match(
        /^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
      );
      if (m) {
        const year = Number(m[1]);
        const month = Number(m[2]) - 1;
        const day = Number(m[3]);
        const h = Number(m[4] || 0);
        const min = Number(m[5] || 0);
        const sec = Number(m[6] || 0);
        const d = new Date(year, month, day, h, min, sec);
        return Number.isNaN(d.getTime()) ? null : d;
      }

      return null;
    }

    function parseNumber_(value) {
      if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
      if (value === null || value === undefined) return 0;
      let cleaned = String(value)
        .replace(/\u00a0/g, '')
        .replace(/\s+/g, '')
        .replace(',', '.')
        .replace(/[^\d.-]/g, '');
      if (/^-?\.\d{3,}$/.test(cleaned)) {
        cleaned = cleaned.replace('.', '');
      }
      if (!cleaned || cleaned === '-' || cleaned === '.') return 0;
      const parsed = Number(cleaned);
      return Number.isFinite(parsed) ? parsed : 0;
    }

    function parseCount_(value) {
      const n = Math.floor(parseNumber_(value));
      return n > 0 ? n : 0;
    }

    function toYesNo_(value) {
      const raw = String(value || '').trim();
      if (!raw) return '';
      const normalized = raw.toLowerCase();
      if (/(да|yes|true|1)/.test(normalized)) return 'Да';
      if (/(нет|no|false|0)/.test(normalized)) return 'Нет';
      return raw;
    }
