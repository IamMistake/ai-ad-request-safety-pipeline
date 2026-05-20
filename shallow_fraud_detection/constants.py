import re


SESSION_WINDOW = 60
LAST_SEEN_WINDOW = 60

MAX_SESSION_FREQ = 30
MOBILE_IP_REPEAT_SECONDS = 3.0
DESKTOP_IP_REPEAT_SECONDS = 2.0

SUSPICIOUS_UA_PENALTY = 0.1
IP_BURST_PENALTY = 0.6
SESSION_BURST_PENALTY = 0.5
NEGATIVE_KEYWORD_PENALTY = 0.7
INVALID_UA_PENALTY = 0.2
LANGUAGE_MISMATCH_PENALTY = 0.2

ALLOW_SCORE_THRESHOLD = 0.7
MAX_FRAUD_SCORE = 1.0
SCORE_DECIMAL_PLACES = 3

NEGATIVE_KEYWORD_PATTERN = re.compile(
    r"\b(wtf|wth|ffs|omfg|shit(ty|tiest)?|dumbass|horrible|awful|"
    r"piss(ed|ing)? off|piece of (shit|crap|junk)|what the (fuck|hell)|"
    r"fucking? (broken|useless|terrible|awful|horrible)|fuck you|"
    r"screw (this|you)|so frustrating|this sucks|damn it)\b"
)

SUSPICIOUS_UA_MARKERS = (
    "curl",
    "python",
    "wget",
    "postmanruntime",
    "bot",
    "spider",
    "crawler",
    "httpclient",
    "java/",
)

VALID_UA_MARKERS = (
    "mozilla/5.0",
    "applewebkit",
    "chrome/",
    "firefox/",
    "safari/",
    "edg/",
    "mobile/",
    "curl/",
    "python-urllib/",
    "wget/",
    "postmanruntime/",
    "googlebot/",
    "bingbot/",
)

LANGUAGE_COUNTRIES = {
    "arabic": {
        "AE",
        "BH",
        "DZ",
        "EG",
        "IQ",
        "JO",
        "KW",
        "LB",
        "LY",
        "MA",
        "OM",
        "QA",
        "SA",
        "SD",
        "SY",
        "TN",
        "YE",
    },
    "chinese": {"CN", "HK", "MO", "SG", "TW"},
    "czech": {"CZ"},
    "danish": {"DK"},
    "dutch": {"BE", "NL", "SR"},
    "finnish": {"FI"},
    "french": {"BE", "CA", "CH", "FR", "LU", "MC", "SN", "CI", "CM", "MA", "DZ", "TN"},
    "german": {"AT", "CH", "DE", "LI", "LU"},
    "greek": {"CY", "GR"},
    "hebrew": {"IL"},
    "hindi": {"IN"},
    "hungarian": {"HU"},
    "indonesian": {"ID"},
    "italian": {"CH", "IT", "SM", "VA"},
    "japanese": {"JP"},
    "korean": {"KR"},
    "malay": {"BN", "MY", "SG"},
    "norwegian": {"NO"},
    "polish": {"PL"},
    "portuguese": {"AO", "BR", "CV", "GW", "MZ", "PT", "ST", "TL"},
    "romanian": {"MD", "RO"},
    "russian": {"BY", "KG", "KZ", "RU"},
    "spanish": {
        "AR",
        "BO",
        "CL",
        "CO",
        "CR",
        "CU",
        "DO",
        "EC",
        "ES",
        "GQ",
        "GT",
        "HN",
        "MX",
        "NI",
        "PA",
        "PE",
        "PR",
        "PY",
        "SV",
        "UY",
        "VE",
    },
    "swedish": {"FI", "SE"},
    "thai": {"TH"},
    "turkish": {"CY", "TR"},
    "ukrainian": {"UA"},
    "vietnamese": {"VN"},
}

LANGUAGE_ALIASES = {
    "en": "english",
    "eng": "english",
    "es": "spanish",
    "spa": "spanish",
    "fr": "french",
    "de": "german",
    "it": "italian",
    "pt": "portuguese",
    "ru": "russian",
    "ja": "japanese",
    "jp": "japanese",
    "ko": "korean",
    "zh": "chinese",
    "ar": "arabic",
    "hi": "hindi",
    "tr": "turkish",
    "nl": "dutch",
    "pl": "polish",
    "uk": "ukrainian",
    "sv": "swedish",
    "no": "norwegian",
    "da": "danish",
    "fi": "finnish",
    "el": "greek",
    "ro": "romanian",
    "hu": "hungarian",
    "he": "hebrew",
    "th": "thai",
    "vi": "vietnamese",
    "id": "indonesian",
    "ms": "malay",
    "cs": "czech",
}
