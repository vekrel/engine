export * from "@vekrel/shared/advertisedEndpoint";

export const machineEndpointUrl = (httpBaseUrl: string, pathname: string): string => {
  const url = new URL(httpBaseUrl);
  url.pathname = pathname;
  url.search = "";
  url.hash = "";
  return url.toString();
};
