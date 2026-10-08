import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { EntityManager } from '@mikro-orm/postgresql';

@Controller('health')
export class HealthController {
  constructor(private readonly em: EntityManager) {}

  @Get('live')
  getLive() {
    return {
      status: 'UP',
      timestamp: new Date().toISOString(),
    };
  }

  @Get('ready')
  async getReady() {
    try {
      const isConnected = await this.em.getConnection().isConnected();
      if (!isConnected) {
        throw new Error('Database is not connected');
      }
      
      await this.em.execute('SELECT 1');

      return {
        status: 'UP',
        database: 'CONNECTED',
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      throw new ServiceUnavailableException({
        status: 'DOWN',
        database: 'DISCONNECTED',
        error: message,
        timestamp: new Date().toISOString(),
      });
    }
  }
}
