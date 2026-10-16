import { Alert, Button, Card, Descriptions, Form, Input, InputNumber, Space } from 'antd';
import { useApp } from '@nocobase/client-v2';
import React, { useState } from 'react';
import { useT } from './locale';

type ConnectionValues = { host: string; port: number; database: string; username: string; password: string };
type TestResult = { success?: boolean; driver?: string; error?: string };

export default function PostgresqlDiagnosticsPage() {
  const app = useApp();
  const t = useT();
  const [form] = Form.useForm<ConnectionValues>();
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<TestResult>();

  const testConnection = async () => {
    const values = await form.validateFields();
    setLoading(true);
    setResult(undefined);
    try {
      const response = await app.apiClient.request({
        url: 'postgresqlConnector:testConnection',
        method: 'post',
        data: values,
      });
      const data = response.data?.data ?? response.data;
      setResult(
        data?.success === true
          ? data
          : { success: false, error: t('PostgreSQL connection test returned an invalid response') },
      );
    } catch (error) {
      setResult({
        success: false,
        error: error instanceof Error ? error.message : t('PostgreSQL connection test failed'),
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <Card title={t('PostgreSQL Connection Test')}>
      <Alert
        type="info"
        showIcon
        message={t('Credentials are submitted only for this test and are not stored.')}
        style={{ marginBottom: 16 }}
      />
      <Form
        form={form}
        layout="vertical"
        initialValues={{ host: 'localhost', port: 5432, database: 'postgres', username: 'postgres' }}
        style={{ maxWidth: 640 }}
      >
        <Form.Item name="host" label={t('Host')} rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="port" label={t('Port')} rules={[{ required: true }]}>
          <InputNumber min={1} max={65535} style={{ width: '100%' }} />
        </Form.Item>
        <Form.Item name="database" label={t('Database')} rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="username" label={t('Username')} rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="password" label={t('Password')} rules={[{ required: true }]}>
          <Input.Password autoComplete="new-password" />
        </Form.Item>
        <Space>
          <Button type="primary" loading={loading} onClick={testConnection}>
            {t('Test Connection')}
          </Button>
        </Space>
      </Form>
      {result && (
        <>
          <Alert
            type={result.success ? 'success' : 'error'}
            showIcon
            message={result.success ? t('PostgreSQL connection succeeded') : t('PostgreSQL connection failed')}
            description={result.error}
            style={{ marginTop: 16 }}
          />
          <Descriptions
            bordered
            size="small"
            style={{ marginTop: 16 }}
            items={[
              { key: 'result', label: t('Result'), children: result.success ? t('Connected') : t('Failed') },
              { key: 'driver', label: t('Driver'), children: result.driver || 'pg' },
              ...(result.error ? [{ key: 'error', label: t('Error'), children: result.error }] : []),
            ]}
          />
        </>
      )}
    </Card>
  );
}
