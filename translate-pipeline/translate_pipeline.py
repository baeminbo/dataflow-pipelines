import logging
from typing import Iterable
from typing import List
from typing import Tuple

from apache_beam import Create
from apache_beam import DoFn
from apache_beam import Map
from apache_beam import ParDo
from apache_beam import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

EN_TEXTS: List[str] = ['good morning', 'good afternoon', 'good evening',
                       'good night']

SOURCE_LANGUAGE_CODE = 'en-US'
TARGET_LANGUAGE_CODE = 'ja'


class TranslateDoFn(DoFn):
  def __init__(self, project, source_language_code, target_language_code):
    DoFn.__init__(self)
    self._project = project
    self._source_language_code = source_language_code
    self._target_language_code = target_language_code

  def setup(self) -> None:
    # using local import for google-cloud-translate so that the package need not
    # to be installed in the local environment creating a Dataflow job. Instead,
    # google-cloud-translate must be included in --requirements_file,
    # --setup_file or --extra_package.
    from google.cloud import translate
    self._client = translate.TranslationServiceClient()

  def process(self, element: str) -> Iterable[Tuple[str, str]]:
    request = dict(parent=f'projects/{self._project}/locations/global',
                   contents=[element], mime_type='text/plain',
                   source_language_code=self._source_language_code,
                   target_language_code=self._target_language_code, )

    response = self._client.translate_text(request)

    if not response.translations:
      logging.warning(
          'Translation response is empty. request: %s, response: %s',
          request, response)
    elif len(response.translations) > 1:
      logging.warning(
          'Translation response has multiple result. request: %s, response: %s',
          request, response)
    else:
      logging.info(
          'Translation succeeded. request: %s, response: %s', request, response)

      yield element, response.translations[0].translated_text


def print_translation(element: Tuple[str, str]) -> None:
  source_text, target_text = element
  logging.info('%s -> %s', source_text, target_text)


def main():
  options = PipelineOptions()
  options.view_as(SetupOptions).save_main_session = True
  project = options.view_as(GoogleCloudOptions).project

  p = Pipeline(options=options)
  (p
   | Create(EN_TEXTS)
   | ParDo(TranslateDoFn(project, SOURCE_LANGUAGE_CODE, TARGET_LANGUAGE_CODE))
   | Map(print_translation))

  p.run()

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
