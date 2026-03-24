import anthropic
from google.cloud import bigquery
from datetime import datetime
import json
import os

# Handle GCP credentials from GitHub Actions secret
gcp_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if gcp_key:
    with open("/tmp/gcp-key.json", "w") as f:
        f.write(gcp_key.strip())
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-key.json"

# --- CLIENTS ---
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
bq = bigquery.Client(project="flights-490708")
TABLE = "flights-490708.flight_data.restricted_airspace"

# Exact sovereign border bounding boxes for every country
# Format: min_lat, max_lat, min_lon, max_lon
# Source: Natural Earth Data (public domain)
KNOWN_BOUNDS = {
    "Afghanistan":                {"min_lat": 29.318572, "max_lat": 38.486281, "min_lon": 60.528429, "max_lon": 75.158027},
    "Albania":                    {"min_lat": 39.624997, "max_lat": 42.688238, "min_lon": 19.304486, "max_lon": 21.020040},
    "Algeria":                    {"min_lat": 18.968147, "max_lat": 37.296205, "min_lon": -8.668908, "max_lon": 11.997337},
    "Angola":                     {"min_lat": -18.038945, "max_lat": -4.388063, "min_lon": 11.460979, "max_lon": 24.087885},
    "Argentina":                  {"min_lat": -55.185076, "max_lat": -21.781168, "min_lon": -73.560032, "max_lon": -53.637451},
    "Armenia":                    {"min_lat": 38.840477, "max_lat": 41.300712, "min_lon": 43.447139, "max_lon": 46.633308},
    "Australia":                  {"min_lat": -43.634597, "max_lat": -10.668185, "min_lon": 113.338953, "max_lon": 153.569469},
    "Austria":                    {"min_lat": 46.431817, "max_lat": 49.039074, "min_lon": 9.479969, "max_lon": 16.979666},
    "Azerbaijan":                 {"min_lat": 38.270377, "max_lat": 41.860675, "min_lon": 44.793989, "max_lon": 50.392821},
    "Bahrain":                    {"min_lat": 25.535000, "max_lat": 26.687244, "min_lon": 50.269798, "max_lon": 50.923369},
    "Bangladesh":                 {"min_lat": 20.670883, "max_lat": 26.446525, "min_lon": 88.007530, "max_lon": 92.680497},
    "Belarus":                    {"min_lat": 51.257598, "max_lat": 56.172180, "min_lon": 23.178334, "max_lon": 32.762780},
    "Belgium":                    {"min_lat": 49.496982, "max_lat": 51.551666, "min_lon": 2.388913, "max_lon": 6.408097},
    "Belize":                     {"min_lat": 15.885728, "max_lat": 18.496001, "min_lon": -89.226208, "max_lon": -87.309849},
    "Benin":                      {"min_lat": 6.039869, "max_lat": 12.409244, "min_lon": 0.776667, "max_lon": 3.843343},
    "Bhutan":                     {"min_lat": 26.702016, "max_lat": 28.246987, "min_lon": 88.746472, "max_lon": 92.125232},
    "Bolivia":                    {"min_lat": -22.898274, "max_lat": -9.668943, "min_lon": -69.645007, "max_lon": -57.453000},
    "Bosnia and Herzegovina":     {"min_lat": 42.555311, "max_lat": 45.276413, "min_lon": 15.728743, "max_lon": 19.623731},
    "Botswana":                   {"min_lat": -26.905966, "max_lat": -17.778137, "min_lon": 19.998647, "max_lon": 29.375304},
    "Brazil":                     {"min_lat": -33.868905, "max_lat": 5.284287, "min_lon": -73.983062, "max_lon": -28.634116},
    "Brunei":                     {"min_lat": 4.002508, "max_lat": 5.101185, "min_lon": 114.075873, "max_lon": 115.363562},
    "Bulgaria":                   {"min_lat": 41.235392, "max_lat": 44.216706, "min_lon": 22.357145, "max_lon": 28.887540},
    "Burkina Faso":               {"min_lat": 9.410471, "max_lat": 15.084000, "min_lon": -5.513241, "max_lon": 2.408971},
    "Burundi":                    {"min_lat": -4.469315, "max_lat": -2.309679, "min_lon": 29.000740, "max_lon": 30.849846},
    "Cambodia":                   {"min_lat": 9.475263, "max_lat": 14.690422, "min_lon": 102.333828, "max_lon": 107.627678},
    "Cameroon":                   {"min_lat": 1.654665, "max_lat": 13.083333, "min_lon": 8.382217, "max_lon": 16.192147},
    "Canada":                     {"min_lat": 41.676555, "max_lat": 83.336212, "min_lon": -141.002750, "max_lon": -52.323198},
    "Central African Republic":   {"min_lat": 2.215655, "max_lat": 11.001389, "min_lon": 14.415542, "max_lon": 27.454076},
    "Chad":                       {"min_lat": 7.441070, "max_lat": 23.497500, "min_lon": 13.473480, "max_lon": 24.000000},
    "Chile":                      {"min_lat": -56.725000, "max_lat": -17.498399, "min_lon": -109.679578, "max_lon": -66.075347},
    "China":                      {"min_lat": 8.838343, "max_lat": 53.560815, "min_lon": 73.499734, "max_lon": 134.775456},
    "Colombia":                   {"min_lat": -4.231687, "max_lat": 16.057126, "min_lon": -82.124366, "max_lon": -66.851190},
    "Congo (Brazzaville)":        {"min_lat": -5.149089, "max_lat": 3.713056, "min_lon": 11.004820, "max_lon": 18.643611},
    "Congo (Kinshasa)":           {"min_lat": -13.459035, "max_lat": 5.392002, "min_lon": 12.039074, "max_lon": 31.305675},
    "Democratic Republic of the Congo (Eastern)": {"min_lat": -13.459035, "max_lat": 5.392002, "min_lon": 12.039074, "max_lon": 31.305675},
    "Costa Rica":                 {"min_lat": 5.332969, "max_lat": 11.219568, "min_lon": -87.272264, "max_lon": -82.506020},
    "Croatia":                    {"min_lat": 42.176599, "max_lat": 46.555029, "min_lon": 13.210481, "max_lon": 19.447084},
    "Cuba":                       {"min_lat": 19.627529, "max_lat": 23.481697, "min_lon": -85.167970, "max_lon": -73.919000},
    "Cyprus":                     {"min_lat": 34.438370, "max_lat": 35.913252, "min_lon": 32.022758, "max_lon": 34.855318},
    "Czech Republic":             {"min_lat": 48.551808, "max_lat": 51.055703, "min_lon": 12.090590, "max_lon": 18.859216},
    "Denmark":                    {"min_lat": 54.451666, "max_lat": 57.952429, "min_lon": 7.715325, "max_lon": 15.553064},
    "Djibouti":                   {"min_lat": 10.914954, "max_lat": 12.792308, "min_lon": 41.771313, "max_lon": 43.657904},
    "Dominican Republic":         {"min_lat": 17.270170, "max_lat": 21.303433, "min_lon": -72.057470, "max_lon": -68.110146},
    "Ecuador":                    {"min_lat": -5.015931, "max_lat": 1.883596, "min_lon": -92.207239, "max_lon": -75.192504},
    "Egypt":                      {"min_lat": 22.000000, "max_lat": 31.833085, "min_lon": 24.649911, "max_lon": 37.115351},
    "El Salvador":                {"min_lat": 12.976046, "max_lat": 14.451048, "min_lon": -90.179097, "max_lon": -87.635139},
    "Equatorial Guinea":          {"min_lat": -1.673219, "max_lat": 3.989000, "min_lon": 5.417294, "max_lon": 11.359862},
    "Eritrea":                    {"min_lat": 12.354821, "max_lat": 18.070991, "min_lon": 36.433365, "max_lon": 43.300171},
    "Estonia":                    {"min_lat": 57.509299, "max_lat": 59.938375, "min_lon": 21.382606, "max_lon": 28.210017},
    "Ethiopia":                   {"min_lat": 3.397448, "max_lat": 14.894053, "min_lon": 32.997583, "max_lon": 47.982379},
    "Finland":                    {"min_lat": 59.454157, "max_lat": 70.092293, "min_lon": 19.083209, "max_lon": 31.586707},
    "France":                     {"min_lat": 41.263218, "max_lat": 51.268318, "min_lon": -5.453428, "max_lon": 9.867834},
    "Gabon":                      {"min_lat": -4.101226, "max_lat": 2.318217, "min_lon": 8.500224, "max_lon": 14.539444},
    "Gambia":                     {"min_lat": 13.061000, "max_lat": 13.825313, "min_lon": -17.028825, "max_lon": -13.797778},
    "Georgia":                    {"min_lat": 41.055292, "max_lat": 43.586429, "min_lon": 39.884480, "max_lon": 46.736537},
    "Germany":                    {"min_lat": 47.270111, "max_lat": 55.099161, "min_lon": 5.866315, "max_lon": 15.041931},
    "Ghana":                      {"min_lat": 4.539252, "max_lat": 11.174856, "min_lon": -3.260786, "max_lon": 1.273294},
    "Greece":                     {"min_lat": 34.700609, "max_lat": 41.748886, "min_lon": 19.247787, "max_lon": 29.729698},
    "Guatemala":                  {"min_lat": 13.634580, "max_lat": 17.816594, "min_lon": -92.310524, "max_lon": -88.175584},
    "Guinea":                     {"min_lat": 7.190604, "max_lat": 12.675630, "min_lon": -15.568050, "max_lon": -7.638199},
    "Guinea-Bissau":              {"min_lat": 10.651421, "max_lat": 12.686238, "min_lon": -16.894523, "max_lon": -13.634877},
    "Guyana":                     {"min_lat": 1.171001, "max_lat": 8.603884, "min_lon": -61.414905, "max_lon": -56.468954},
    "Haiti":                      {"min_lat": 17.909929, "max_lat": 19.915684, "min_lon": -75.238461, "max_lon": -71.624873},
    "Honduras":                   {"min_lat": 12.980848, "max_lat": 17.619526, "min_lon": -89.356820, "max_lon": -82.172962},
    "Hungary":                    {"min_lat": 45.759481, "max_lat": 48.623854, "min_lon": 16.202298, "max_lon": 22.710531},
    "Iceland":                    {"min_lat": 63.496382, "max_lat": 66.526792, "min_lon": -24.326184, "max_lon": -13.609732},
    "India":                      {"min_lat": 7.965534, "max_lat": 35.494009, "min_lon": 68.176645, "max_lon": 97.402561},
    "Indonesia":                  {"min_lat": -10.359987, "max_lat": 5.479820, "min_lon": 95.293026, "max_lon": 141.033851},
    "Iran":                       {"min_lat": 24.846510, "max_lat": 39.781650, "min_lon": 44.031890, "max_lon": 63.333270},
    "Iraq":                       {"min_lat": 29.058566, "max_lat": 37.380932, "min_lon": 38.793671, "max_lon": 48.841270},
    "Ireland":                    {"min_lat": 51.669301, "max_lat": 55.131622, "min_lon": -9.977085, "max_lon": -6.032985},
    "Israel":                     {"min_lat": 29.453379, "max_lat": 33.335631, "min_lon": 34.267499, "max_lon": 35.895023},
    "Italy":                      {"min_lat": 36.619987, "max_lat": 47.115393, "min_lon": 6.749955, "max_lon": 18.480247},
    "Ivory Coast":                {"min_lat": 4.338288, "max_lat": 10.524060, "min_lon": -8.602880, "max_lon": -2.562189},
    "Jamaica":                    {"min_lat": 17.701116, "max_lat": 18.524218, "min_lon": -78.337719, "max_lon": -76.199658},
    "Japan":                      {"min_lat": 31.029579, "max_lat": 45.551483, "min_lon": 129.408463, "max_lon": 145.543137},
    "Jordan":                     {"min_lat": 29.197494, "max_lat": 33.378686, "min_lon": 34.922602, "max_lon": 39.195468},
    "Kazakhstan":                 {"min_lat": 40.568647, "max_lat": 55.442170, "min_lon": 46.493217, "max_lon": 87.315631},
    "Kenya":                      {"min_lat": -4.899520, "max_lat": 4.620000, "min_lon": 33.909898, "max_lon": 41.899578},
    "Kosovo":                     {"min_lat": 41.857540, "max_lat": 43.268614, "min_lon": 20.014755, "max_lon": 21.788250},
    "Kuwait":                     {"min_lat": 28.526062, "max_lat": 30.059069, "min_lon": 46.568713, "max_lon": 48.416094},
    "Kyrgyzstan":                 {"min_lat": 39.172843, "max_lat": 43.266797, "min_lon": 69.264952, "max_lon": 80.229579},
    "Laos":                       {"min_lat": 13.909675, "max_lat": 22.508671, "min_lon": 100.084324, "max_lon": 107.634998},
    "Latvia":                     {"min_lat": 55.674650, "max_lat": 58.085568, "min_lon": 20.671540, "max_lon": 28.241490},
    "Lebanon":                    {"min_lat": 33.089040, "max_lat": 34.644914, "min_lon": 35.126052, "max_lon": 36.611750},
    "Lesotho":                    {"min_lat": -30.677277, "max_lat": -28.570615, "min_lon": 27.011463, "max_lon": 29.455709},
    "Liberia":                    {"min_lat": 4.155590, "max_lat": 8.551986, "min_lon": -11.608076, "max_lon": -7.367323},
    "Libya":                      {"min_lat": 19.500813, "max_lat": 33.354589, "min_lon": 9.391081, "max_lon": 25.377062},
    "Lithuania":                  {"min_lat": 53.896789, "max_lat": 56.450421, "min_lon": 20.653783, "max_lon": 26.835519},
    "Luxembourg":                 {"min_lat": 49.442667, "max_lat": 50.128051, "min_lon": 5.674051, "max_lon": 6.242751},
    "Macedonia":                  {"min_lat": 40.842726, "max_lat": 42.373535, "min_lon": 20.452902, "max_lon": 23.034051},
    "Madagascar":                 {"min_lat": -25.601434, "max_lat": -12.040556, "min_lon": 43.254187, "max_lon": 50.476536},
    "Malawi":                     {"min_lat": -17.129603, "max_lat": -9.368326, "min_lon": 32.670361, "max_lon": 35.918573},
    "Malaysia":                   {"min_lat": -5.107624, "max_lat": 9.892375, "min_lon": 105.347193, "max_lon": 120.347193},
    "Mali":                       {"min_lat": 10.147811, "max_lat": 25.001084, "min_lon": -12.240283, "max_lon": 4.267382},
    "Mauritania":                 {"min_lat": 14.616834, "max_lat": 27.395744, "min_lon": -17.063423, "max_lon": -4.923337},
    "Mexico":                     {"min_lat": 14.538828, "max_lat": 32.720830, "min_lon": -117.127760, "max_lon": -86.811982},
    "Moldova":                    {"min_lat": 45.488283, "max_lat": 48.467119, "min_lon": 26.619336, "max_lon": 30.024658},
    "Mongolia":                   {"min_lat": 41.597409, "max_lat": 52.047366, "min_lon": 87.751264, "max_lon": 119.772823},
    "Montenegro":                 {"min_lat": 41.877550, "max_lat": 43.523840, "min_lon": 18.450000, "max_lon": 20.339800},
    "Morocco":                    {"min_lat": 21.420734, "max_lat": 35.759988, "min_lon": -17.020428, "max_lon": -1.124551},
    "Mozambique":                 {"min_lat": -26.742191, "max_lat": -10.317096, "min_lon": 30.179481, "max_lon": 40.775475},
    "Myanmar":                    {"min_lat": 9.932959, "max_lat": 28.335945, "min_lon": 92.303234, "max_lon": 101.180005},
    "Namibia":                    {"min_lat": -29.045461, "max_lat": -16.941342, "min_lon": 11.734198, "max_lon": 25.084443},
    "Nepal":                      {"min_lat": 26.397898, "max_lat": 30.422716, "min_lon": 80.088424, "max_lon": 88.174804},
    "Netherlands":                {"min_lat": 50.803721, "max_lat": 53.510403, "min_lon": 3.314971, "max_lon": 7.092053},
    "New Zealand":                {"min_lat": -46.641235, "max_lat": -34.450661, "min_lon": 166.509144, "max_lon": 178.517093},
    "Nicaragua":                  {"min_lat": 10.707656, "max_lat": 15.033118, "min_lon": -87.901532, "max_lon": -82.622702},
    "Niger":                      {"min_lat": 11.660167, "max_lat": 23.471658, "min_lon": 0.295646, "max_lon": 15.903246},
    "Nigeria":                    {"min_lat": 4.069095, "max_lat": 13.885645, "min_lon": 2.676932, "max_lon": 14.678014},
    "North Korea":                {"min_lat": 37.586785, "max_lat": 43.008964, "min_lon": 124.091390, "max_lon": 130.924647},
    "Norway":                     {"min_lat": 58.078884, "max_lat": 80.657144, "min_lon": 4.992078, "max_lon": 31.293418},
    "Oman":                       {"min_lat": 16.651051, "max_lat": 26.395934, "min_lon": 52.000009, "max_lon": 59.808060},
    "Pakistan":                   {"min_lat": 23.691965, "max_lat": 37.133030, "min_lon": 60.874248, "max_lon": 77.837450},
    "Pakistan (Northwest/Baluchistan)": {"min_lat": 23.691965, "max_lat": 37.133030, "min_lon": 60.874248, "max_lon": 77.837450},
    "Palestine":                  {"min_lat": 31.220128, "max_lat": 32.552147, "min_lon": 34.068973, "max_lon": 35.573923},
    "Panama":                     {"min_lat": 7.220541, "max_lat": 9.611610, "min_lon": -82.965783, "max_lon": -77.242566},
    "Papua New Guinea":           {"min_lat": -10.652476, "max_lat": -2.500002, "min_lon": 141.000210, "max_lon": 156.019965},
    "Paraguay":                   {"min_lat": -27.548499, "max_lat": -19.342746, "min_lon": -62.685057, "max_lon": -54.292959},
    "Peru":                       {"min_lat": -18.347975, "max_lat": -0.057205, "min_lon": -81.410942, "max_lon": -68.665079},
    "Philippines":                {"min_lat": 4.215806, "max_lat": 21.321780, "min_lon": 114.095214, "max_lon": 126.807256},
    "Poland":                     {"min_lat": 49.002046, "max_lat": 55.033696, "min_lon": 14.122970, "max_lon": 24.145783},
    "Portugal":                   {"min_lat": 36.838268, "max_lat": 42.280468, "min_lon": -9.526570, "max_lon": -6.389087},
    "Qatar":                      {"min_lat": 24.556330, "max_lat": 26.114582, "min_lon": 50.743910, "max_lon": 51.606700},
    "Romania":                    {"min_lat": 43.688444, "max_lat": 48.220881, "min_lon": 20.220192, "max_lon": 29.626543},
    "Russia":                     {"min_lat": 41.185096, "max_lat": 82.058623, "min_lon": 19.638900, "max_lon": 180.000000},
    "Russia (Western)":           {"min_lat": 41.185096, "max_lat": 82.058623, "min_lon": 19.638900, "max_lon": 60.000000},
    "Rwanda":                     {"min_lat": -2.838980, "max_lat": -1.047408, "min_lon": 28.861754, "max_lon": 30.899073},
    "Saudi Arabia":               {"min_lat": 16.347891, "max_lat": 32.161008, "min_lon": 34.632336, "max_lon": 55.666659},
    "Senegal":                    {"min_lat": 12.237283, "max_lat": 16.691971, "min_lon": -17.786241, "max_lon": -11.345899},
    "Serbia":                     {"min_lat": 42.232243, "max_lat": 46.190052, "min_lon": 18.814287, "max_lon": 23.006309},
    "Sierra Leone":               {"min_lat": 6.755000, "max_lat": 9.999973, "min_lon": -13.500338, "max_lon": -10.271683},
    "Slovakia":                   {"min_lat": 47.731428, "max_lat": 49.613816, "min_lon": 16.833189, "max_lon": 22.565710},
    "Slovenia":                   {"min_lat": 45.421424, "max_lat": 46.876681, "min_lon": 13.375469, "max_lon": 16.596770},
    "Somalia":                    {"min_lat": -1.683250, "max_lat": 12.024640, "min_lon": 40.981050, "max_lon": 51.133870},
    "South Africa":               {"min_lat": -34.819166, "max_lat": -22.091312, "min_lon": 16.344976, "max_lon": 32.830120},
    "South Korea":                {"min_lat": 34.390045, "max_lat": 38.612242, "min_lon": 126.117397, "max_lon": 129.468304},
    "South Sudan":                {"min_lat": 3.509170, "max_lat": 12.248007, "min_lon": 23.886979, "max_lon": 35.298007},
    "Spain":                      {"min_lat": 27.433542, "max_lat": 43.993308, "min_lon": -18.393684, "max_lon": 4.591888},
    "Sri Lanka":                  {"min_lat": 5.968369, "max_lat": 9.824077, "min_lon": 79.695166, "max_lon": 81.787959},
    "Sudan":                      {"min_lat": 8.619729, "max_lat": 22.000000, "min_lon": 21.936810, "max_lon": 38.410089},
    "Suriname":                   {"min_lat": 1.831280, "max_lat": 6.225000, "min_lon": -58.070833, "max_lon": -53.843335},
    "Sweden":                     {"min_lat": 55.361737, "max_lat": 69.106247, "min_lon": 11.027368, "max_lon": 23.903378},
    "Switzerland":                {"min_lat": 45.776947, "max_lat": 47.830827, "min_lon": 6.022609, "max_lon": 10.442701},
    "Syria":                      {"min_lat": 32.312937, "max_lat": 37.229872, "min_lon": 35.700797, "max_lon": 42.349591},
    "Taiwan":                     {"min_lat": 21.970571, "max_lat": 25.295458, "min_lon": 120.106188, "max_lon": 121.951243},
    "Tajikistan":                 {"min_lat": 36.671115, "max_lat": 41.045093, "min_lon": 67.333277, "max_lon": 75.153956},
    "Tanzania":                   {"min_lat": -11.720938, "max_lat": -0.950000, "min_lon": 29.339997, "max_lon": 40.316590},
    "Thailand":                   {"min_lat": 5.612851, "max_lat": 20.464833, "min_lon": 97.343807, "max_lon": 105.636812},
    "Timor-Leste":                {"min_lat": -9.393173, "max_lat": -8.273344, "min_lon": 124.968682, "max_lon": 127.335928},
    "Togo":                       {"min_lat": 5.926547, "max_lat": 11.139510, "min_lon": -0.143974, "max_lon": 1.808760},
    "Trinidad and Tobago":        {"min_lat": 9.873210, "max_lat": 11.562837, "min_lon": -62.083056, "max_lon": -60.289584},
    "Tunisia":                    {"min_lat": 30.230236, "max_lat": 37.761205, "min_lon": 7.521980, "max_lon": 11.880113},
    "Turkey":                     {"min_lat": 35.807680, "max_lat": 42.297000, "min_lon": 25.621289, "max_lon": 44.817663},
    "Turkmenistan":               {"min_lat": 35.129093, "max_lat": 42.797557, "min_lon": 52.335076, "max_lon": 66.689517},
    "Uganda":                     {"min_lat": -1.482317, "max_lat": 4.234076, "min_lon": 29.573433, "max_lon": 35.000308},
    "Ukraine":                    {"min_lat": 44.184598, "max_lat": 52.379147, "min_lon": 22.137059, "max_lon": 40.227580},
    "United Arab Emirates":       {"min_lat": 22.496947, "max_lat": 26.055464, "min_lon": 51.579518, "max_lon": 56.396847},
    "United Kingdom":             {"min_lat": 49.674000, "max_lat": 61.061000, "min_lon": -14.015517, "max_lon": 2.091911},
    "United States":              {"min_lat": 24.949300, "max_lat": 49.590400, "min_lon": -125.001100, "max_lon": -66.932600},
    "Uruguay":                    {"min_lat": -35.782448, "max_lat": -30.085396, "min_lon": -58.494843, "max_lon": -53.075583},
    "Uzbekistan":                 {"min_lat": 37.182116, "max_lat": 45.590118, "min_lon": 55.997786, "max_lon": 73.139736},
    "Venezuela":                  {"min_lat": 0.647529,  "max_lat": 15.915843, "min_lon": -73.352963, "max_lon": -59.542707},
    "Vietnam":                    {"min_lat": 8.179066, "max_lat": 23.393395, "min_lon": 102.144410, "max_lon": 114.333759},
    "Yemen":                      {"min_lat": 12.585950, "max_lat": 19.000003, "min_lon": 42.604872, "max_lon": 53.108572},
    "Zambia":                     {"min_lat": -17.961228, "max_lat": -8.271282, "min_lon": 21.999350, "max_lon": 33.701111},
    "Zimbabwe":                   {"min_lat": -22.424109, "max_lat": -15.609703, "min_lon": 25.237300, "max_lon": 33.068341},
}


def ask_claude_for_restrictions():
    response = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        tools=[{"type": "web_search_20250305", "name": "web_search"}],
        messages=[{
            "role": "user",
            "content": """Search the web for countries with currently restricted or closed 
            airspace in 2026 due to conflict, war, or military activity. 
            Use sources like safeairspace.net, FAA NOTAMs, and EASA advisories.
            Return ONLY a raw JSON array, no markdown, no explanation.

            CRITICAL COORDINATE RULES:
            - Use the actual sovereign border coordinates of the country ONLY
            - Do NOT expand the bounding box to cover surrounding conflict regions
            - Do NOT include neighboring countries in the bounding box
            - Bounding boxes must be tight to the country's actual borders, not the broader affected region

            EXAMPLES OF CORRECT vs INCORRECT:
            - Iran: correct is (~29.0 to 39.8 lat, 44.0 to 63.3 lon), NOT 25.0 lat which bleeds into Saudi Arabia
            - Ukraine: correct is (~44.4 to 52.4 lat, 22.1 to 40.2 lon), NOT broader bounds that bleed into Poland or Romania
            - Apply this same precision to every country in the list

            Each object must have exactly these fields:
            {
              "country": "string",
              "reason": "string (one sentence)",
              "min_lat": float,
              "max_lat": float,
              "min_lon": float,
              "max_lon": float,
              "severity": "CLOSED or RESTRICTED or HIGH RISK",
              "since": "YYYY-MM-DD"
            }"""
        }]
    )

    text = ""
    for block in response.content:
        if hasattr(block, "text"):
            text += block.text

    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
    if text.startswith("json"):
        text = text[4:]
    text = text.strip()
    return json.loads(text)


def validate_zones(zones):
    for zone in zones:
        country = zone.get("country", "")
        if country in KNOWN_BOUNDS:
            bounds = KNOWN_BOUNDS[country]
            original = (zone["min_lat"], zone["max_lat"], zone["min_lon"], zone["max_lon"])
            zone["min_lat"] = max(zone["min_lat"], bounds["min_lat"])
            zone["max_lat"] = min(zone["max_lat"], bounds["max_lat"])
            zone["min_lon"] = max(zone["min_lon"], bounds["min_lon"])
            zone["max_lon"] = min(zone["max_lon"], bounds["max_lon"])
            updated = (zone["min_lat"], zone["max_lat"], zone["min_lon"], zone["max_lon"])
            if original != updated:
                print(f"  Corrected bounds for {country}")
    return zones


def refresh_bigquery(zones):
    now = datetime.utcnow().isoformat()
    for zone in zones:
        zone["updated_at"] = now

    bq.query(f"DELETE FROM `{TABLE}` WHERE TRUE").result()
    print(f"Cleared existing data")

    errors = bq.insert_rows_json(TABLE, zones)
    if not errors:
        print(f"Inserted {len(zones)} restricted zones into BigQuery")
        for z in zones:
            print(f"  {z['severity']:12} | {z['country']:20} | {z['reason'][:60]}")
    else:
        print(f"BigQuery errors: {errors}")


if __name__ == "__main__":
    print(f"--- Starting airspace update {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ---")
    zones = ask_claude_for_restrictions()
    print(f"Claude found {len(zones)} restricted zones")
    zones = validate_zones(zones)
    print(f"Validated bounding boxes")
    refresh_bigquery(zones)
    print(f"--- Done {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} ---")