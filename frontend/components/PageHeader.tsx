import { ReactNode } from 'react';
import './PageHeader.css';

interface PageHeaderProps {
  title: ReactNode;
  description?: string;
  titleClassName?: string;
}

export default function PageHeader({ title, description: _description, titleClassName }: PageHeaderProps) {
  return (
    <div className="page-header">
      <h1 className={['page-header-title', titleClassName].filter(Boolean).join(' ')}>{title}</h1>
    </div>
  );
}
