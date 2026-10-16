import { useApp } from '@nocobase/client-v2';
import type { DataSourceSettingsFormProps } from '@nocobase/plugin-data-source-manager/src/client-v2/plugin';
import { Button, Checkbox, Form, Input, InputNumber, Select } from 'antd';
import React, { useEffect, useState } from 'react';
import { useT } from './locale';

const MAX_OBJECTS = 500;

type PostgresqlObject = { name: string; tableName: string; schema?: string; view?: boolean };

export default function PostgresqlSettingsForm(props: DataSourceSettingsFormProps) {
  const t = useT();
  const app = useApp();
  const form = Form.useFormInstance();
  const [objects, setObjects] = useState<PostgresqlObject[]>([]);
  const [loading, setLoading] = useState(false);

  async function loadObjects() {
    const values = await form.validateFields(['key', 'options']);
    setLoading(true);
    try {
      const response = await app.apiClient.request({
        url: 'postgresqlConnector:readTables',
        method: 'post',
        data: { dataSourceKey: values.key, options: values.options },
      });
      setObjects((response?.data?.data || response?.data || []).slice(0, MAX_OBJECTS));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    const selectedTables = props.initialValues?.options?.selectedTables;
    const collections = (props.initialValues?.collections || []).flatMap((item): PostgresqlObject[] => {
      if (typeof item === 'string') return [{ name: item, tableName: item }];
      const collection = item.options || item;
      return collection.name && collection.tableName ? [collection] : [];
    });
    const persistedSelection = Array.isArray(selectedTables)
      ? selectedTables.map((name) => String(name))
      : collections.map((collection) => collection.name);
    if (collections.length) setObjects(collections);
    form.setFieldValue(['options', 'selectedTables'], persistedSelection);
  }, [form, props.initialValues]);

  return (
    <>
      <Form.Item name={['options', 'host']} label={t('Host')} rules={[{ required: true }]}>
        <Input />
      </Form.Item>
      <Form.Item name={['options', 'port']} label={t('Port')} initialValue={5432}>
        <InputNumber min={1} max={65535} style={{ width: '100%' }} />
      </Form.Item>
      <Form.Item name={['options', 'database']} label={t('Database')} rules={[{ required: true }]}>
        <Input />
      </Form.Item>
      <Form.Item name={['options', 'username']} label={t('Username')} rules={[{ required: true }]}>
        <Input />
      </Form.Item>
      <Form.Item name={['options', 'schema']} label={t('Schema')} initialValue="public">
        <Input />
      </Form.Item>
      <Form.Item
        name={['options', 'password']}
        label={t('Password')}
        rules={props.mode === 'create' ? [{ required: true }] : []}
      >
        <Input.Password autoComplete="new-password" />
      </Form.Item>
      <Form.Item name={['options', 'tablePrefix']} label={t('Table Prefix')}>
        <Input />
      </Form.Item>
      <Form.Item name={['options', 'ssl']} valuePropName="checked" initialValue={false}>
        <Checkbox>{t('Use SSL')}</Checkbox>
      </Form.Item>
      <Button onClick={loadObjects} loading={loading}>
        {t('Load collections')}
      </Button>
      <Form.Item
        name={['options', 'selectedTables']}
        label={t('Collections')}
        rules={[{ required: true, message: t('Collections') }]}
      >
        <Select
          mode="tags"
          maxCount={MAX_OBJECTS}
          options={objects.map((object) => ({
            value: object.name,
            label: `${object.name}${object.view ? ' (view)' : ''}`,
          }))}
          placeholder={t('Load collections')}
          showSearch
        />
      </Form.Item>
    </>
  );
}
