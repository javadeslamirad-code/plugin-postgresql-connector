import { Application, Plugin } from '@nocobase/client-v2';
import type { PluginDataSourceManagerClientV2 } from '@nocobase/plugin-data-source-manager/src/client-v2/plugin';
import PostgresqlSettingsForm from './PostgresqlSettingsForm';

export class PluginPostgresqlConnectorClientV2 extends Plugin<never, Application> {
  async load() {
    const manager = (this.app.pm.get('@nocobase/plugin-data-source-manager') ||
      this.app.pm.get('data-source-manager')) as PluginDataSourceManagerClientV2 | undefined;
    manager?.registerType('postgresql', {
      label: String(this.t('PostgreSQL')),
      defaultValues: { type: 'postgresql', options: { port: 5432, schema: 'public', selectedTables: [] } },
      disableAddFields: true,
      capabilities: { query: true, runSQL: false },
      normalizeValues: (values) => {
        const options = values.options || {};
        const selectedTables = Array.isArray(options.selectedTables)
          ? options.selectedTables.map((name) => String(name).trim()).filter(Boolean)
          : [];
        return { ...values, options: { ...options, selectedTables }, collections: selectedTables };
      },
      SettingsForm: PostgresqlSettingsForm,
    });
    this.pluginSettingsManager.addMenuItem({
      key: 'postgresql-connector',
      title: this.t('PostgreSQL Connector'),
      icon: 'DatabaseOutlined',
    });
    this.pluginSettingsManager.addPageTabItem({
      menuKey: 'postgresql-connector',
      key: 'diagnostics',
      title: this.t('Connection Test'),
      componentLoader: () => import('./PostgresqlDiagnosticsPage'),
    });
  }
}

export default PluginPostgresqlConnectorClientV2;
