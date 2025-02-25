from typing import Dict

from django_server.display_center.models import Config


class ConfigRepository:

    def save(self, config: Dict) -> None:
        c = Config(id=config.get('id'), param1=config.get('param1'), param2=config.get('param2'),
                   param3=config.get('param3'))
        c.save()

    def find_by_id(self, config_id: str):
        config_obj = Config.objects.filter(id=config_id).first()
        return config_obj

    def update(self, config: Dict):
        Config.objects.filter(id=config.get('id')).update(param1=config.get('param1'), param2=config.get('param2'),
                                                          param3=config.get('param3'))
