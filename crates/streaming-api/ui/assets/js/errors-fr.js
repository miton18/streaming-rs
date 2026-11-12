// French messages of the API errors, by code. `{name}` is replaced by the
// value `name` of the answer's context, exactly as the server builds its
// English `message` (a test of streaming-api checks that every code is here
// with the same placeholders).

export const MESSAGES = {
  IDENTIFIER_EMPTY: 'le paramètre {parameter} ne doit pas être vide',
  IDENTIFIER_TOO_LONG: 'le paramètre {parameter} `{value}` dépasse {maxLength} octets',
  IDENTIFIER_INVALID_CHARACTER: 'le paramètre {parameter} `{value}` ne peut contenir que des lettres, des chiffres, `-` et `_`',
  BODY_UNREADABLE: "le corps de la requête n'a pas pu être lu",
  BODY_TOO_LARGE: 'le corps de la requête est trop volumineux',
  BODY_NOT_JSON: "le corps de la requête n'est pas du JSON valide (ligne {line}, colonne {column})",
  BODY_UNKNOWN_FIELD: 'le corps de la requête contient un champ inconnu `{field}` (ligne {line}, colonne {column})',
  BODY_MISSING_FIELD: 'il manque le champ `{field}` dans le corps de la requête (ligne {line}, colonne {column})',
  BODY_INVALID: 'le corps de la requête est invalide : {detail} (ligne {line}, colonne {column})',
  LIMIT_INVALID: 'limite `{value}` invalide : un nombre de lignes est attendu',
  QUERY_INVALID: 'les paramètres de la requête sont invalides : {detail}',
  TENANT_NOT_FOUND: 'tenant `{tenant}` inconnu',
  TENANT_NOT_CREATED: "le tenant `{tenant}` n'existe pas : créez-le avant d'y déposer des graphes",
  TENANT_DISPLAY_NAME_CONFLICT: 'le tenant `{tenant}` existe déjà avec un autre nom affiché',
  DISPLAY_NAME_TOO_LONG: 'le nom affiché dépasse {maxLength} caractères',
  DISPLAY_NAME_INVALID_CHARACTER: 'le nom affiché contient un caractère de contrôle',
  GRAPH_NOT_FOUND: 'graphe `{tenant}/{graph}` inconnu',
  NODE_NOT_FOUND: 'nœud `{node}` inconnu dans le graphe `{tenant}/{graph}`',
  GRAPH_KEY_MISMATCH: 'la description concerne `{bodyTenant}/{bodyGraph}` mais le chemin désigne `{tenant}/{graph}`',
  GRAPH_BEING_DELETED: 'le graphe `{tenant}/{graph}` est en cours de suppression',
  NODE_CONFIG_NOT_OBJECT: "la configuration du nœud `{node}` du graphe `{tenant}/{graph}` n'est pas un objet et ne peut pas être modifiée",
  GRAPH_EMPTY: "le graphe n'a aucun nœud",
  NODE_DUPLICATED: 'le nœud `{node}` est déclaré deux fois',
  EDGE_UNKNOWN_NODE: "l'arête `{from}` -> `{to}` désigne un nœud inconnu, `{unknown}`",
  EDGE_INVALID: "l'arête `{from}` -> `{to}` est en double ou boucle sur un même nœud",
  GRAPH_CYCLE: 'le graphe contient un cycle passant par {nodes}',
  OPERATOR_UNKNOWN: 'le nœud `{node}` désigne un opérateur inconnu, `{op}`',
  TYPE_MISMATCH: "arête `{from}` -> `{to}` : `{from}` produit `{produced}` mais `{to}` attend `{expected}`",
  WIRING_SOURCE_HAS_INPUTS: "nœud `{node}` ({kind}) : une source ne peut pas avoir d'entrée",
  WIRING_SOURCE_WITHOUT_OUTPUTS: 'nœud `{node}` ({kind}) : une source doit avoir au moins une sortie',
  WIRING_SINK_WITHOUT_INPUTS: 'nœud `{node}` ({kind}) : un puits doit avoir au moins une entrée',
  WIRING_SINK_HAS_OUTPUTS: 'nœud `{node}` ({kind}) : un puits ne peut pas avoir de sortie',
  WIRING_NEEDS_INPUT_AND_OUTPUT: 'nœud `{node}` ({kind}) : il faut au moins une entrée et une sortie',
  WIRING_KEY_BY_MUST_FEED_WINDOWS: 'nœud `{node}` ({kind}) : un nœud key_by ne doit alimenter que des fenêtres',
  WIRING_WINDOW_MUST_BE_FED_BY_KEY_BY: 'nœud `{node}` ({kind}) : une fenêtre ne doit être alimentée que par des nœuds key_by',
  WIRING_WINDOW_MUST_FEED_AGGREGATES: "nœud `{node}` ({kind}) : une fenêtre ne doit alimenter que des agrégats",
  WIRING_AGGREGATE_NEEDS_ONE_WINDOW: 'nœud `{node}` ({kind}) : un agrégat doit avoir exactement une entrée, une fenêtre',
  WIRING_AGGREGATE_WITHOUT_OUTPUTS: 'nœud `{node}` ({kind}) : un agrégat doit avoir au moins une sortie',
  DOCUMENT_TOO_LARGE: 'le document fait {size} octets, au-delà de la limite du stockage ({limit})',
  STORE_UNAVAILABLE: 'le stockage ne répond pas, réessayez plus tard',
  STORE_DOCUMENT_UNREADABLE: 'un document du stockage est illisible',
  ROUTE_NOT_FOUND: "rien n'est servi à l'adresse `{path}`",
  UI_NOT_EMBEDDED: "l'interface web n'est pas embarquée dans ce binaire",
};

function textOf(value) {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return value.map(textOf).join(', ');
  return JSON.stringify(value);
}

/** `template` with the context's values in its placeholders. */
export function render(template, context = {}) {
  return template.replace(/\{([^{}]*)\}/g, (all, name) => (Object.hasOwn(context, name) ? textOf(context[name]) : all));
}

/**
 * The French message of an error answer, or null when its code is unknown
 * (a newer server): the caller then shows the server's English message.
 */
export function messageOf(body) {
  if (!body || typeof body !== 'object' || typeof body.code !== 'string') return null;
  const template = MESSAGES[body.code];
  return template === undefined ? null : render(template, body.context && typeof body.context === 'object' ? body.context : {});
}
