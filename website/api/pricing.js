'use strict';

const BASE_PRICES = Object.freeze({
  pro: 99,
  proPlus: 199,
  proUltra: 349,
});

const RATE_URL = 'https://open.er-api.com/v6/latest/INR';
const RATE_CACHE_TTL_MS = 1000 * 60 * 60 * 6;

let cachedRates = null;
let cachedRatesAt = 0;

const CURRENCY_BY_COUNTRY = {};

function assignCurrency(currency, countries) {
  countries.forEach((country) => {
    CURRENCY_BY_COUNTRY[country] = currency;
  });
}

assignCurrency('USD', ['AS', 'BQ', 'EC', 'FM', 'GU', 'MH', 'MP', 'PA', 'PR', 'PW', 'SV', 'TC', 'TL', 'UM', 'US', 'VI', 'VG', 'ZW']);
assignCurrency('EUR', ['AD', 'AT', 'AX', 'BE', 'BL', 'CY', 'DE', 'EE', 'ES', 'FI', 'FR', 'GF', 'GP', 'GR', 'HR', 'IE', 'IT', 'LT', 'LU', 'LV', 'MC', 'ME', 'MF', 'MQ', 'MT', 'NL', 'PM', 'PT', 'RE', 'SI', 'SK', 'SM', 'TF', 'VA', 'XK', 'YT']);
assignCurrency('GBP', ['GB', 'GG', 'IM', 'JE']);
assignCurrency('AUD', ['AU', 'CC', 'CX', 'HM', 'KI', 'NF', 'NR', 'TV']);
assignCurrency('NZD', ['CK', 'NU', 'NZ', 'PN', 'TK']);
assignCurrency('CAD', ['CA']);
assignCurrency('CHF', ['CH', 'LI']);
assignCurrency('DKK', ['DK', 'FO', 'GL']);
assignCurrency('NOK', ['NO', 'SJ']);
assignCurrency('SEK', ['SE']);
assignCurrency('ISK', ['IS']);
assignCurrency('PLN', ['PL']);
assignCurrency('CZK', ['CZ']);
assignCurrency('HUF', ['HU']);
assignCurrency('RON', ['RO']);
assignCurrency('BGN', ['BG']);
assignCurrency('RSD', ['RS']);
assignCurrency('BAM', ['BA']);
assignCurrency('MKD', ['MK']);
assignCurrency('ALL', ['AL']);
assignCurrency('GEL', ['GE']);
assignCurrency('AMD', ['AM']);
assignCurrency('AZN', ['AZ']);
assignCurrency('MDL', ['MD']);
assignCurrency('UAH', ['UA']);
assignCurrency('BYN', ['BY']);
assignCurrency('RUB', ['RU']);
assignCurrency('TRY', ['TR']);
assignCurrency('ILS', ['IL', 'PS']);
assignCurrency('JOD', ['JO']);
assignCurrency('LBP', ['LB']);
assignCurrency('SYP', ['SY']);
assignCurrency('IQD', ['IQ']);
assignCurrency('IRR', ['IR']);
assignCurrency('YER', ['YE']);
assignCurrency('AED', ['AE']);
assignCurrency('SAR', ['SA']);
assignCurrency('QAR', ['QA']);
assignCurrency('KWD', ['KW']);
assignCurrency('BHD', ['BH']);
assignCurrency('OMR', ['OM']);
assignCurrency('EGP', ['EG']);
assignCurrency('MAD', ['EH', 'MA']);
assignCurrency('DZD', ['DZ']);
assignCurrency('TND', ['TN']);
assignCurrency('LYD', ['LY']);
assignCurrency('SDG', ['SD']);
assignCurrency('SSP', ['SS']);
assignCurrency('ETB', ['ET']);
assignCurrency('ERN', ['ER']);
assignCurrency('DJF', ['DJ']);
assignCurrency('SOS', ['SO']);
assignCurrency('KES', ['KE']);
assignCurrency('UGX', ['UG']);
assignCurrency('TZS', ['TZ']);
assignCurrency('RWF', ['RW']);
assignCurrency('BIF', ['BI']);
assignCurrency('XAF', ['CF', 'CG', 'CM', 'GA', 'GQ', 'TD']);
assignCurrency('XOF', ['BJ', 'BF', 'CI', 'GW', 'ML', 'NE', 'SN', 'TG']);
assignCurrency('GHS', ['GH']);
assignCurrency('NGN', ['NG']);
assignCurrency('SLE', ['SL']);
assignCurrency('GMD', ['GM']);
assignCurrency('GNF', ['GN']);
assignCurrency('LRD', ['LR']);
assignCurrency('CVE', ['CV']);
assignCurrency('STN', ['ST']);
assignCurrency('MRU', ['MR']);
assignCurrency('AOA', ['AO']);
assignCurrency('CDF', ['CD']);
assignCurrency('ZAR', ['ZA']);
assignCurrency('LSL', ['LS']);
assignCurrency('NAD', ['NA']);
assignCurrency('BWP', ['BW']);
assignCurrency('SZL', ['SZ']);
assignCurrency('ZMW', ['ZM']);
assignCurrency('MWK', ['MW']);
assignCurrency('MZN', ['MZ']);
assignCurrency('MGA', ['MG']);
assignCurrency('SCR', ['SC']);
assignCurrency('MUR', ['MU']);
assignCurrency('KMF', ['KM']);
assignCurrency('XPF', ['NC', 'PF', 'WF']);
assignCurrency('FJD', ['FJ']);
assignCurrency('PGK', ['PG']);
assignCurrency('SBD', ['SB']);
assignCurrency('TOP', ['TO']);
assignCurrency('WST', ['WS']);
assignCurrency('VUV', ['VU']);
assignCurrency('JPY', ['JP']);
assignCurrency('CNY', ['CN']);
assignCurrency('HKD', ['HK']);
assignCurrency('MOP', ['MO']);
assignCurrency('TWD', ['TW']);
assignCurrency('KRW', ['KR']);
assignCurrency('SGD', ['SG']);
assignCurrency('MYR', ['MY']);
assignCurrency('THB', ['TH']);
assignCurrency('IDR', ['ID']);
assignCurrency('PHP', ['PH']);
assignCurrency('VND', ['VN']);
assignCurrency('KHR', ['KH']);
assignCurrency('LAK', ['LA']);
assignCurrency('MMK', ['MM']);
assignCurrency('BND', ['BN']);
assignCurrency('MNT', ['MN']);
assignCurrency('KZT', ['KZ']);
assignCurrency('KGS', ['KG']);
assignCurrency('TJS', ['TJ']);
assignCurrency('TMT', ['TM']);
assignCurrency('UZS', ['UZ']);
assignCurrency('AFN', ['AF']);
assignCurrency('PKR', ['PK']);
assignCurrency('INR', ['IN']);
assignCurrency('BDT', ['BD']);
assignCurrency('NPR', ['NP']);
assignCurrency('LKR', ['LK']);
assignCurrency('BTN', ['BT']);
assignCurrency('MVR', ['MV']);
assignCurrency('MXN', ['MX']);
assignCurrency('GTQ', ['GT']);
assignCurrency('HNL', ['HN']);
assignCurrency('NIO', ['NI']);
assignCurrency('CRC', ['CR']);
assignCurrency('BZD', ['BZ']);
assignCurrency('BBD', ['BB']);
assignCurrency('BSD', ['BS']);
assignCurrency('BMD', ['BM']);
assignCurrency('JMD', ['JM']);
assignCurrency('TTD', ['TT']);
assignCurrency('HTG', ['HT']);
assignCurrency('DOP', ['DO']);
assignCurrency('CUP', ['CU']);
assignCurrency('AWG', ['AW']);
assignCurrency('ANG', ['CW', 'SX']);
assignCurrency('KYD', ['KY']);
assignCurrency('GYD', ['GY']);
assignCurrency('SRD', ['SR']);
assignCurrency('BRL', ['BR']);
assignCurrency('ARS', ['AR']);
assignCurrency('CLP', ['CL']);
assignCurrency('COP', ['CO']);
assignCurrency('PEN', ['PE']);
assignCurrency('BOB', ['BO']);
assignCurrency('PYG', ['PY']);
assignCurrency('UYU', ['UY']);
assignCurrency('VES', ['VE']);
assignCurrency('FKP', ['FK']);
assignCurrency('SHP', ['SH']);
assignCurrency('XCD', ['AG', 'AI', 'DM', 'GD', 'KN', 'LC', 'MS', 'VC']);
assignCurrency('GIP', ['GI']);

function normalizeCode(value, expectedLength) {
  if (typeof value !== 'string') {
    return '';
  }

  const normalized = value.trim().toUpperCase();
  const expression = expectedLength === 2 ? /^[A-Z]{2}$/ : /^[A-Z]{3}$/;
  return expression.test(normalized) ? normalized : '';
}

function getSearchParams(req) {
  try {
    return new URL(req.url, 'https://clinx.local').searchParams;
  } catch (error) {
    return new URL('https://clinx.local').searchParams;
  }
}

function inferCountryFromAcceptLanguage(acceptLanguage) {
  if (typeof acceptLanguage !== 'string' || !acceptLanguage.trim()) {
    return '';
  }

  const firstLocale = acceptLanguage.split(',')[0].trim();
  if (!firstLocale) {
    return '';
  }

  try {
    const locale = new Intl.Locale(firstLocale);
    return normalizeCode(locale.region || '', 2);
  } catch (error) {
    const match = firstLocale.match(/-([A-Za-z]{2})$/);
    return match ? normalizeCode(match[1], 2) : '';
  }
}

function getCountry(req, searchParams) {
  const override = normalizeCode(searchParams.get('country'), 2);
  if (override) {
    return override;
  }

  const vercelCountry = normalizeCode(req.headers['x-vercel-ip-country'], 2);
  if (vercelCountry) {
    return vercelCountry;
  }

  const acceptLanguageCountry = inferCountryFromAcceptLanguage(req.headers['accept-language']);
  return acceptLanguageCountry || 'US';
}

function getTargetCurrency(searchParams, country) {
  const override = normalizeCode(searchParams.get('currency'), 3);
  if (override) {
    return override;
  }

  return CURRENCY_BY_COUNTRY[country] || 'USD';
}

async function getRatePayload() {
  if (cachedRates && Date.now() - cachedRatesAt < RATE_CACHE_TTL_MS) {
    return cachedRates;
  }

  const response = await fetch(RATE_URL, {
    headers: {
      accept: 'application/json',
    },
  });

  if (!response.ok) {
    throw new Error(`Rate request failed with ${response.status}`);
  }

  const payload = await response.json();
  if (payload.result !== 'success' || !payload.rates) {
    throw new Error('Unexpected rate payload');
  }

  cachedRates = payload;
  cachedRatesAt = Date.now();
  return payload;
}

function convertPrices(currency, rates) {
  const multiplier = rates[currency];
  if (typeof multiplier !== 'number') {
    return null;
  }

  return Object.fromEntries(
    Object.entries(BASE_PRICES).map(([plan, amount]) => {
      return [plan, Number((amount * multiplier).toFixed(2))];
    })
  );
}

module.exports = async (req, res) => {
  const searchParams = getSearchParams(req);
  const country = getCountry(req, searchParams);
  let displayCurrency = getTargetCurrency(searchParams, country);
  let plans = { ...BASE_PRICES };
  let pricingMode = displayCurrency === 'INR' ? 'base' : 'converted';
  let fallback = false;
  let updatedAt = null;

  if (displayCurrency !== 'INR') {
    try {
      const ratePayload = await getRatePayload();
      updatedAt = ratePayload.time_last_update_utc || null;

      let convertedPlans = convertPrices(displayCurrency, ratePayload.rates);
      if (!convertedPlans) {
        displayCurrency = 'USD';
        fallback = true;
        convertedPlans = convertPrices(displayCurrency, ratePayload.rates);
      }

      if (convertedPlans) {
        plans = convertedPlans;
      } else {
        displayCurrency = 'INR';
        plans = { ...BASE_PRICES };
        pricingMode = 'base';
        fallback = true;
      }
    } catch (error) {
      displayCurrency = 'INR';
      plans = { ...BASE_PRICES };
      pricingMode = 'base';
      fallback = true;
    }
  }

  const payload = {
    baseCurrency: 'INR',
    country,
    displayCurrency,
    pricingMode,
    fallback,
    updatedAt,
    plans,
    provider: 'ExchangeRate-API',
    providerUrl: 'https://www.exchangerate-api.com',
  };

  res.statusCode = 200;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'private, max-age=1800');
  res.end(JSON.stringify(payload));
};
