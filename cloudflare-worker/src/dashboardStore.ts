/**
 * Dashboard artifact store (deep module).
 *
 * Encapsulates the `__`-prefixed R2 key layout for per-project dashboard
 * artifacts. Callers must never construct raw `__*` keys: the resolver and
 * its tests depend on this module as the single place that knows the key
 * convention.
 *
 * Every getter returns `null` on miss and never throws.
 */

export interface DashboardStore {
  /**
   * Fetch the rendered dashboard HTML for a project.
   *
   * @param project - Project slug.
   * @returns The R2 object body on hit, or `null` on miss / R2 error.
   */
  getDashboard(project: string): Promise<R2ObjectBody | null>;

  /**
   * Fetch the version-switcher JSON for a project.
   *
   * @param project - Project slug.
   * @returns The R2 object body on hit, or `null` on miss / R2 error.
   */
  getSwitcher(project: string): Promise<R2ObjectBody | null>;

  /**
   * Fetch the per-edition metadata JSON (canonical URL, `is_canonical`) for
   * a given edition of a project.
   *
   * @param project - Project slug.
   * @param edition - Edition name.
   * @returns The R2 object body on hit, or `null` on miss / R2 error.
   */
  getEditionMeta(
    project: string,
    edition: string,
  ): Promise<R2ObjectBody | null>;
}

/**
 * Build a DashboardStore backed by the given R2 bucket.
 *
 * The returned store owns the key layout so callers never see raw
 * `__`-prefixed keys.
 */
export function createDashboardStore(r2: R2Bucket): DashboardStore {
  return {
    async getDashboard(project: string): Promise<R2ObjectBody | null> {
      try {
        return await r2.get(`${project}/__dashboard.html`);
      } catch {
        return null;
      }
    },
    async getSwitcher(project: string): Promise<R2ObjectBody | null> {
      try {
        return await r2.get(`${project}/__switcher.json`);
      } catch {
        return null;
      }
    },
    async getEditionMeta(
      project: string,
      edition: string,
    ): Promise<R2ObjectBody | null> {
      try {
        return await r2.get(`${project}/__editions/${edition}.json`);
      } catch {
        return null;
      }
    },
  };
}
